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

#include "common/status.h"
#include "exec/iceberg-metadata/iceberg-metadata-table-scanner.h"
#include "runtime/timestamp-value.inline.h"
#include "util/jni-util.h"
#include "runtime/runtime-state.h"

using namespace strings;

namespace impala {

IcebergMetadataTableScanner::IcebergMetadataTableScanner(
    const TupleDescriptor* tuple_desc,
    const string* metadata_table_name,
    std::vector<ScalarExprEvaluator*> conjunct_evals)
  : tuple_desc_(tuple_desc),
    metadata_table_name_(metadata_table_name),
    conjunct_evals_(conjunct_evals),
    tuple_idx_(0) {}

Status IcebergMetadataTableScanner::Init(JNIEnv* env) {
  // Global class references:
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/impala/catalog/FeIcebergTable", &fe_iceberg_table_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/Table", &iceberg_api_table_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/MetadataTableUtils", &iceberg_metadata_table_utils_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/TableScan", &iceberg_table_scan_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/MetadataTableType", &iceberg_metadata_table_type_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/Scan", &iceberg_scan_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/FileScanTask", &iceberg_file_scan_task_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/DataTask", &iceberg_data_task_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/io/CloseableIterable", &iceberg_closeable_iterable_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/io/CloseableIterator", &iceberg_closeable_iterator_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/StructLike", &iceberg_struct_like_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/Accessor", &iceberg_accessor_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/Schema", &iceberg_schema_cl_));
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
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, fe_iceberg_table_cl_, "getIcebergApiTable",
      "()Lorg/apache/iceberg/Table;", &fe_iceberg_table_get_iceberg_api_table_));
  RETURN_IF_ERROR(JniUtil::GetStaticMethodID(env, iceberg_metadata_table_utils_cl_,
      "createMetadataTableInstance", "(Lorg/apache/iceberg/Table;Lorg/apache/iceberg/"
          "MetadataTableType;)Lorg/apache/iceberg/Table;",
      &iceberg_metadata_table_utils_create_metadata_table_instance_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_api_table_cl_, "newScan",
      "()Lorg/apache/iceberg/TableScan;", &iceberg_table_new_scan_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_scan_cl_, "planFiles",
      "()Lorg/apache/iceberg/io/CloseableIterable;", &iceberg_table_scan_plan_files_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_closeable_iterable_cl_, "iterator",
      "()Lorg/apache/iceberg/io/CloseableIterator;",
      &iceberg_closable_iterable_iterator_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_closeable_iterator_cl_, "hasNext",
      "()Z", &iceberg_closeable_iterator_has_next_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_closeable_iterator_cl_, "next",
      "()Ljava/lang/Object;", &iceberg_closeable_iterator_next_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_data_task_cl_, "rows",
      "()Lorg/apache/iceberg/io/CloseableIterable;", &iceberg_data_task_rows_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_api_table_cl_, "schema",
      "()Lorg/apache/iceberg/Schema;", &iceberg_table_schema_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_schema_cl_, "columns",
      "()Ljava/util/List;", &iceberg_schema_columns_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, list_cl_, "get",
      "(I)Ljava/lang/Object;", &list_get_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_nested_field_cl_, "fieldId",
      "()I", &iceberg_nested_field_field_id_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_schema_cl_, "accessorForField",
      "(I)Lorg/apache/iceberg/Accessor;", &iceberg_schema_accessor_for_field_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_accessor_cl_, "get",
      "(Ljava/lang/Object;)Ljava/lang/Object;", &iceberg_accessor_get_));
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
  RETURN_IF_ERROR(CreateJIcebergMetadataTable(env, fe_table));
  RETURN_IF_ERROR(CreateJAccessors(env));
  return Status::OK();
}

Status IcebergMetadataTableScanner::CreateJIcebergMetadataTable(JNIEnv* env,
    jobject* jtable) {
  DCHECK(jtable != NULL);
  // FeIcebergTable.getIcebergApiTable()
  jobject jiceberg_api_table = env->CallObjectMethod(*jtable,
      fe_iceberg_table_get_iceberg_api_table_);
  RETURN_ERROR_IF_EXC(env);
  // Get the MetadataTableType enum's field
  jfieldID metadata_table_type_field = env->GetStaticFieldID(
      iceberg_metadata_table_type_cl_, metadata_table_name_->c_str(),
      "Lorg/apache/iceberg/MetadataTableType;");
  RETURN_ERROR_IF_EXC(env);
  // Get the object of MetadataTableType enum field
  jobject metadata_table_type_object = env->GetStaticObjectField(
      iceberg_metadata_table_type_cl_, metadata_table_type_field);
  RETURN_ERROR_IF_EXC(env);
  // org.apache.iceberg.Table
  jobject metadata_table = env->CallStaticObjectMethod(iceberg_metadata_table_type_cl_,
      iceberg_metadata_table_utils_create_metadata_table_instance_, jiceberg_api_table,
      metadata_table_type_object);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, metadata_table, jmetadata_table_));
  return Status::OK();
}

Status IcebergMetadataTableScanner::ScanMetadataTable(JNIEnv* env) {
  // metadataTable.newScan();
  jobject scan =  env->CallObjectMethod(*jmetadata_table_, iceberg_table_new_scan_);
  RETURN_ERROR_IF_EXC(env);
  // scan.planFiles()
  jobject file_scan_task_iterable = env->CallObjectMethod(scan,
      iceberg_table_scan_plan_files_);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, file_scan_task_iterable,
      &file_scan_task_iterable_));
  // scan.planFiles().iterator()
  jobject file_scan_task_iterator = env->CallObjectMethod(file_scan_task_iterable_,
      iceberg_closable_iterable_iterator_);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, file_scan_task_iterator,
      &file_scan_task_iterator_));
  return Status::OK();
}

Status IcebergMetadataTableScanner::CreateJAccessors(JNIEnv* env) {
  for (SlotDescriptor* slot_desc: tuple_desc_->slots()) {
    // metadataTable.schema()
    jobject schema = env->CallObjectMethod(*jmetadata_table_, iceberg_table_schema_);
    RETURN_ERROR_IF_EXC(env);
    // metadataTable.schema().columns()
    jobject columns = env->CallObjectMethod(schema, iceberg_schema_columns_);
    RETURN_ERROR_IF_EXC(env);
    // metadataTable.schema().columns().get(column_id)
    jobject column_field = env->CallObjectMethod(columns, list_get_, slot_desc->col_pos());
    RETURN_ERROR_IF_EXC(env);
    // metadataTable.schema().columns().get(column_id).fieldId()
    jint column_field_id = env->CallIntMethod(column_field,
        iceberg_nested_field_field_id_);
    RETURN_ERROR_IF_EXC(env);
    // metadataTable.schema().accessorForField(field_id)
    jobject accessor_for_field = env->CallObjectMethod(schema,
        iceberg_schema_accessor_for_field_, column_field_id);
    RETURN_ERROR_IF_EXC(env);
    jaccessors_[slot_desc->col_pos()] = accessor_for_field;
  }
  return Status::OK();
}

Status IcebergMetadataTableScanner::GetNext(JNIEnv* env, RowBatch* row_batch,
    RuntimeState* state, bool* eos) {
  // TODO: DCHECK/TIMERS
  RETURN_IF_CANCELLED(state);
  // Allocate buffer for RowBatch and init the tuple
  uint8_t* tuple_buffer;
  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buffer_size,
      &tuple_buffer));
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
  tuple->Init(tuple_buffer_size);

  // file_scan_task_iterator.hasNext()
  while(env->CallBooleanMethod(file_scan_task_iterator_,
      iceberg_closeable_iterator_has_next_)) {
    RETURN_ERROR_IF_EXC(env);
    // fileScanTaskIterator.next()
    jobject data_task = env->CallObjectMethod(file_scan_task_iterator_,
        iceberg_closeable_iterator_next_);
    RETURN_ERROR_IF_EXC(env);
    // dataTask.rows()
    jobject data_rows_iterable = env->CallObjectMethod(data_task,
        iceberg_data_task_rows_);
    RETURN_ERROR_IF_EXC(env);
    // dataTask.rows().iterator()
    jobject data_rows_iterator = env->CallObjectMethod(data_rows_iterable,
        iceberg_closable_iterable_iterator_);
    RETURN_ERROR_IF_EXC(env);
    // dataTaskIterator.hasNext()
    while(env->CallBooleanMethod(data_rows_iterator,
        iceberg_closeable_iterator_has_next_)) {
      RETURN_ERROR_IF_EXC(env);
      if (row_batch->AtCapacity()) {
        return Status::OK();
        LOG(INFO) << "TMATE: num_rows_:" << num_rows_;
      }

      int row_idx = row_batch->AddRow();
      TupleRow* tuple_row = row_batch->GetRow(row_idx);
      tuple_row->SetTuple(tuple_idx_, tuple);
      // dataTaskIterator.next()
      jobject struct_like_row = env->CallObjectMethod(data_rows_iterator,
          iceberg_closeable_iterator_next_);
      RETURN_ERROR_IF_EXC(env);
      // Translate a StructLikeRow from Iceberg to Tuple
      RETURN_IF_ERROR(MaterializeNextRow(env, struct_like_row, tuple, state, row_batch->tuple_data_pool()));
      // Evaluate conjuncts on this tuple row
      if (ExecNode::EvalConjuncts(conjunct_evals_.data(), conjunct_evals_.size(), tuple_row)) {
        LOG(INFO) << "TMATE: positive eval";
        row_batch->CommitLastRow();
        num_rows_++;
        tuple = reinterpret_cast<Tuple*>(
            reinterpret_cast<uint8_t*>(tuple) + tuple_desc_->byte_size());
      } else {
        // Reset the null bits, everyhing else will be overwritten
        LOG(INFO) << "TMATE: null bits reseted";
        Tuple::ClearNullBits(tuple, tuple_desc_->null_bytes_offset(), tuple_desc_->num_null_bytes());
      }
    }
  }
  LOG(INFO) << "TMATE: num_rows_:" << num_rows_  << " row_batch: " << row_batch->num_rows();
  *eos = true;
  return Status::OK();
}

Status IcebergMetadataTableScanner::MaterializeNextRow(JNIEnv* env, jobject struct_like_row,
    Tuple* tuple, RuntimeState* state, MemPool* tuple_data_pool) {
  for (SlotDescriptor* slot_desc: tuple_desc_->slots()) {
    LOG(INFO) << "TMATE: " << "SLOT_ID: " << slot_desc->id() << " SLOT_TYPE: " << slot_desc->type().type;
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
        RETURN_IF_ERROR(ReadStringValue(env, slot_desc, struct_like_row, tuple, tuple_data_pool));
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
  LOG(INFO) << "TMATE: LONGSLOT:" << result;
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