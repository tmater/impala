// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/exec-node.inline.h"
#include "exec/iceberg-metadata/iceberg-metadata-table-scanner.h"
#include "exec/iceberg-metadata/iceberg-metadata-scan-node.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/tuple-row.h"

using namespace strings;

namespace impala {

IcebergMetadataTableScanner::IcebergMetadataTableScanner(
    IcebergMetadataScanNode* scan_node,
    RuntimeState* state,
    const TupleDescriptor* tuple_desc,
    const string* metadata_table_name)
  : scan_node_(scan_node),
    state_(state),
    tuple_desc_(tuple_desc),
    metadata_table_name_(metadata_table_name),
    tuple_idx_(0),
    scan_prepare_timer_(ADD_TIMER(scan_node_->runtime_profile(), "ScanPrepareTime")),
    iceberg_api_scan_timer_(ADD_TIMER(scan_node_->runtime_profile(),
        "IcebergApiScanTime")) {}

Status IcebergMetadataTableScanner::Init(JNIEnv* env) {
  // Global class references:
  
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/impala/util/IcebergMetadataScanner",
      &impala_iceberg_metadata_scanner_cl_));

  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/StructLike", &iceberg_struct_like_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/Accessor", &iceberg_accessor_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "java/util/List", &list_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/types/Types$NestedField", &iceberg_nested_field_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "java/lang/Boolean", &java_boolean_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "java/lang/Integer", &java_int_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "java/lang/Long", &java_long_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "java/lang/CharSequence", &java_char_sequence_cl_));

  // Method ids:
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "<init>", "(Lorg/apache/impala/catalog/FeTable;Ljava/lang/String;)V", &init_iceberg_metadata_scanner_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "ScanMetadataTable", "()V", &scan_metadata_table_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "GetAccessor", "(I)Lorg/apache/iceberg/Accessor;", &get_accessor_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "GetNext", "()Lorg/apache/iceberg/StructLike;", &get_next_));

  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_accessor_cl_, "get",
      "(Ljava/lang/Object;)Ljava/lang/Object;", &iceberg_accessor_get_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, list_cl_, "get",
      "(I)Ljava/lang/Object;", &list_get_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_boolean_cl_, "booleanValue", "()Z",
      &boolean_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_int_cl_, "intValue", "()I",
      &int_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_long_cl_, "longValue", "()J",
      &long_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_char_sequence_cl_, "toString",
      "()Ljava/lang/String;", &char_sequence_value_));
  return Status::OK();
}

Status IcebergMetadataTableScanner::Prepare(JNIEnv* env, jobject* fe_table) {
  SCOPED_TIMER(scan_prepare_timer_);
  // Create Java Scanner
  jstring str = env->NewStringUTF(metadata_table_name_->c_str());
  jobject jmetadata_scanner = env->NewObject(impala_iceberg_metadata_scanner_cl_,
      init_iceberg_metadata_scanner_, *fe_table, str);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, jmetadata_scanner, &jmetadata_scanner_));
  env->CallObjectMethod(jmetadata_scanner_, scan_metadata_table_);
  RETURN_ERROR_IF_EXC(env);
  for (SlotDescriptor* slot_desc: tuple_desc_->slots()) {
    jobject accessor_for_field = env->CallObjectMethod(jmetadata_scanner_,
        get_accessor_, slot_desc->col_pos());
    RETURN_ERROR_IF_EXC(env);
    jobject accessor_for_field_global_ref;
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, accessor_for_field,
        &accessor_for_field_global_ref));
    RETURN_ERROR_IF_EXC(env);
    jaccessors_[slot_desc->col_pos()] = accessor_for_field_global_ref;
  }
  return Status::OK();
}

Status IcebergMetadataTableScanner::GetNext(JNIEnv* env, RowBatch* row_batch, bool* eos) {
  RETURN_IF_CANCELLED(state_);
  SCOPED_TIMER(scan_node_->materialize_tuple_timer());
  // Allocate buffer for RowBatch and init the tuple
  uint8_t* tuple_buffer;
  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(row_batch->ResizeAndAllocateTupleBuffer(state_, &tuple_buffer_size,
      &tuple_buffer));
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
  tuple->Init(tuple_buffer_size);
  bool hasNext = true;
  while(hasNext) {
    RETURN_ERROR_IF_EXC(env);
    if (row_batch->AtCapacity() || !hasNext) {
      if (!hasNext) {
        *eos = true;
      }
      return Status::OK();
    }

    int row_idx = row_batch->AddRow();
    TupleRow* tuple_row = row_batch->GetRow(row_idx);
    tuple_row->SetTuple(tuple_idx_, tuple);
    // dataTaskIterator.next()
    LOG(INFO) << "TMATE";
    // jobject struct_like_row = env->CallObjectMethod(data_rows_iterator_,
    //     iceberg_closeable_iterator_next_);

    // TODO:        WUUUUUUUUU
    jobject struct_like_row = env->CallObjectMethod(jmetadata_scanner_, get_next_);
    if (struct_like_row == nullptr) {
      *eos = true;
      return Status::OK();
    }
    LOG(INFO) << "TMATE";
    RETURN_ERROR_IF_EXC(env);
    // Translate a StructLikeRow from Iceberg to Tuple
    RETURN_IF_ERROR(MaterializeNextRow(env, struct_like_row, tuple,
        row_batch->tuple_data_pool()));
    LOG(INFO) << "TMATE";
    COUNTER_ADD(scan_node_->rows_read_counter(), 1);

    // Evaluate conjuncts on this tuple row
    if (ExecNode::EvalConjuncts(scan_node_->conjunct_evals().data(),
        scan_node_->conjunct_evals().size(), tuple_row)) {
      row_batch->CommitLastRow();
      tuple = reinterpret_cast<Tuple*>(
          reinterpret_cast<uint8_t*>(tuple) + tuple_desc_->byte_size());
    } else {
      // Reset the null bits, everyhing else will be overwritten
      Tuple::ClearNullBits(tuple, tuple_desc_->null_bytes_offset(),
          tuple_desc_->num_null_bytes());
    }
  }
  LOG(INFO) << "TMATE: Set EOS 1";
  *eos = true;
  return Status::OK();
}

Status IcebergMetadataTableScanner::MaterializeNextRow(JNIEnv* env,
    jobject struct_like_row, Tuple* tuple, MemPool* tuple_data_pool) {
  for (SlotDescriptor* slot_desc: tuple_desc_->slots()) {
    switch (slot_desc->type().type) {
      case TYPE_BOOLEAN: { // java.lang.Boolean
        RETURN_IF_ERROR(ReadBooleanValue(env, slot_desc, struct_like_row, tuple));
        break;
      } case TYPE_INT: { // java.lang.Integer
        RETURN_IF_ERROR(ReadIntValue(env, slot_desc, struct_like_row, tuple));
        break;
      } case TYPE_BIGINT: { // java.lang.Long
        RETURN_IF_ERROR(ReadLongValue(env, slot_desc, struct_like_row, tuple));
        break;
      } case TYPE_TIMESTAMP: { // org.apache.iceberg.types.TimestampType
        RETURN_IF_ERROR(ReadTimeStampValue(env, slot_desc, struct_like_row, tuple));
        break;
      } case TYPE_STRING: { // java.lang.String
        RETURN_IF_ERROR(ReadStringValue(env, slot_desc, struct_like_row, tuple,
            tuple_data_pool));
        break;
      }
      default:
        // Skip the unsupported type and set it to NULL
        tuple->SetNull(slot_desc->null_indicator_offset());
        LOG(INFO) << "Skipping unsupported column type: " << slot_desc->type().type;
    }
  }
  return Status::OK();
}

Status IcebergMetadataTableScanner::ReadBooleanValue(JNIEnv* env,
    SlotDescriptor* slot_desc, jobject struct_like_row, Tuple* tuple) {
  jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()],
      iceberg_accessor_get_, struct_like_row);
  RETURN_ERROR_IF_EXC(env);
  if (accessed_value == nullptr) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  jboolean result = env->CallBooleanMethod(accessed_value, boolean_value_);
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  *reinterpret_cast<uint8_t*>(slot) = reinterpret_cast<uint8_t>(result);
  return Status::OK();
}

Status IcebergMetadataTableScanner::ReadIntValue(JNIEnv* env,
    SlotDescriptor* slot_desc, jobject struct_like_row, Tuple* tuple) {
  // accessor.get()
  jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()],
      iceberg_accessor_get_, struct_like_row);
  RETURN_ERROR_IF_EXC(env);
  if (accessed_value == nullptr) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  // ((Integer)accessedValue).intValue()
  jint result = env->CallIntMethod(accessed_value, int_value_);
  RETURN_ERROR_IF_EXC(env);
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  *reinterpret_cast<int64_t*>(slot) = reinterpret_cast<int32_t>(result);
  return Status::OK();
}

Status IcebergMetadataTableScanner::ReadLongValue(JNIEnv* env,
    SlotDescriptor* slot_desc, jobject struct_like_row, Tuple* tuple) {
  // accessor.get()
  jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()],
      iceberg_accessor_get_, struct_like_row);
  RETURN_ERROR_IF_EXC(env);
  if (accessed_value == nullptr) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  // ((Long)accessedValue).longValue()
  jlong result = env->CallLongMethod(accessed_value, long_value_);
  RETURN_ERROR_IF_EXC(env);
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  *reinterpret_cast<int64_t*>(slot) = reinterpret_cast<int64_t>(result);
  return Status::OK();
}

Status IcebergMetadataTableScanner::ReadTimeStampValue(JNIEnv* env,
    SlotDescriptor* slot_desc, jobject struct_like_row, Tuple* tuple) {
  // accessor.get()
  jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()],
      iceberg_accessor_get_, struct_like_row);
  RETURN_ERROR_IF_EXC(env);
  if (accessed_value == nullptr) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  // ((Long)accessedValue).longValue()
  jlong result = env->CallLongMethod(accessed_value, long_value_);
  RETURN_ERROR_IF_EXC(env);
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  *reinterpret_cast<TimestampValue*>(slot) = TimestampValue::FromUnixTimeMicros(result,
      UTCPTR);
  return Status::OK();
}

Status IcebergMetadataTableScanner::ReadStringValue(JNIEnv* env,
    SlotDescriptor* slot_desc, jobject struct_like_row, Tuple* tuple,
    MemPool* tuple_data_pool) {
  // accessor.get()
  jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()],
      iceberg_accessor_get_, struct_like_row);
  RETURN_ERROR_IF_EXC(env);
  if (accessed_value == nullptr) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  // ((Long)accessedValue).charSequenceValue()
  jstring result = (jstring)env->CallObjectMethod(accessed_value, char_sequence_value_);
  RETURN_ERROR_IF_EXC(env);
  JniUtfCharGuard str_guard;
  RETURN_IF_ERROR(JniUtfCharGuard::create(env, result, &str_guard));
  // Allocate memory and copy the string from the JVM to the RowBatch
  int str_len = strlen(str_guard.get());
  char* buffer = reinterpret_cast<char*>(tuple_data_pool->TryAllocateUnaligned(str_len));
  if (UNLIKELY(buffer == nullptr)) {
    string details = Substitute("Failed to allocate $1 bytes for string.", str_len);
    return tuple_data_pool->mem_tracker()->MemLimitExceeded(NULL, details, str_len);
  }
  memcpy(buffer, str_guard.get(), str_len);
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  reinterpret_cast<StringValue*>(slot)->ptr = buffer;
  reinterpret_cast<StringValue*>(slot)->len = str_len;
  return Status::OK();
}

}