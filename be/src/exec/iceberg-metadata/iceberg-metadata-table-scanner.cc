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

namespace impala {

IcebergMetadataTableScanner::IcebergMetadataTableScanner(
    const TupleDescriptor* tuple_desc,
    const string* metadata_table_name,
    std::vector<ScalarExpr*> conjuncts,
    std::vector<ScalarExprEvaluator*> conjunct_evals)
  : tuple_desc_(tuple_desc),
    metadata_table_name_(metadata_table_name),
    conjuncts_(conjuncts),
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

  // Method ids
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, fe_iceberg_table_cl_, "getIcebergApiTable",
      "()Lorg/apache/iceberg/Table;", &fe_iceberg_table_get_iceberg_api_table_));
  RETURN_IF_ERROR(JniUtil::GetStaticMethodID(env, iceberg_metadata_table_utils_cl_,
      "createMetadataTableInstance",
      "(Lorg/apache/iceberg/Table;Lorg/apache/iceberg/MetadataTableType;)"
          "Lorg/apache/iceberg/Table;",
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
  jobject jiceberg_api_table = env->CallObjectMethod(*jtable,
      fe_iceberg_table_get_iceberg_api_table_);
  jfieldID metadata_table_type_field = env->GetStaticFieldID(
      iceberg_metadata_table_type_cl_, metadata_table_name_->c_str(),
      "Lorg/apache/iceberg/MetadataTableType;");
  jobject metadata_table_type_object = env->GetStaticObjectField(
      iceberg_metadata_table_type_cl_, metadata_table_type_field);
  jobject metadata_table = env->CallStaticObjectMethod(iceberg_metadata_table_type_cl_,
      iceberg_metadata_table_utils_create_metadata_table_instance_, jiceberg_api_table,
      metadata_table_type_object);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, metadata_table, jmetadata_table_));
  return Status::OK();
}

Status IcebergMetadataTableScanner::ScanMetadataTable(JNIEnv* env) {
  // metadataTable.newScan();
  jobject scan =  env->CallObjectMethod(*jmetadata_table_, iceberg_table_new_scan_);
  // scan.planFiles()
  jobject file_scan_task_iterable = env->CallObjectMethod(scan,
      iceberg_table_scan_plan_files_);
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
  while(env->CallBooleanMethod(file_scan_task_iterator_,
      iceberg_closeable_iterator_has_next_)) {
    RETURN_ERROR_IF_EXC(env);
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
    // data_rows_iterator.hasNext()
    bool committed = true;
    while(env->CallBooleanMethod(data_rows_iterator,
        iceberg_closeable_iterator_has_next_)) {
      RETURN_ERROR_IF_EXC(env);
      if (row_batch->AtCapacity()) {
        return Status::OK();
      }
      uint8_t* tuple_buffer;
      int64_t tuple_buffer_size;
      TupleRow* row;
      Tuple* tuple;
      if (committed) {
        RETURN_IF_ERROR(row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buffer_size,
            &tuple_buffer));
        tuple = reinterpret_cast<Tuple*>(tuple_buffer);
        tuple->Init(tuple_buffer_size);
        int row_idx = row_batch->AddRow();
        row = row_batch->GetRow(row_idx);
        row->SetTuple(tuple_idx_, tuple);
      }
      jobject struct_like_row = env->CallObjectMethod(data_rows_iterator,
          iceberg_closeable_iterator_next_);
      RETURN_IF_ERROR(GetRow(env, struct_like_row, tuple, state, row_batch->tuple_data_pool()));
      if (ExecNode::EvalConjuncts(conjunct_evals_.data(), conjunct_evals_.size(), row)) {
        row_batch->CommitLastRow();
        committed = true;
      } else {
        committed = false;
      }
    }
  }
  *eos = true;
  return Status::OK();
}

Status IcebergMetadataTableScanner::GetRow(JNIEnv* env, jobject struct_like_row,
    Tuple* tuple, RuntimeState* state, MemPool* tuple_data_pool) {
  for (SlotDescriptor* slot_desc: tuple_desc_->slots()) {
    LOG(INFO) << "slot type: " << slot_desc->type().type << " accessor_id: ";
    switch (slot_desc->type().type) {
      case TYPE_BOOLEAN: { // java.lang.Boolean
        RETURN_IF_ERROR(ReadBooleanValue(env, tuple, struct_like_row, slot_desc));
        break;
      } case TYPE_INT: { // java.lang.Integer
        RETURN_IF_ERROR(ReadIntValue(env, tuple, struct_like_row, slot_desc));
        break;
      } case TYPE_BIGINT: { // java.lang.Long
        RETURN_IF_ERROR(ReadLongValue(env, tuple, struct_like_row, slot_desc));
        break;
      } case TYPE_TIMESTAMP: { // org.apache.iceberg.types.TimestampType
        RETURN_IF_ERROR(ReadTimeStampValue(env, tuple, struct_like_row, slot_desc));
        break;
      } case TYPE_STRING: { // java.lang.String
        RETURN_IF_ERROR(ReadStringValue(env, tuple, struct_like_row, slot_desc, tuple_data_pool));
        break;
      }
      default:
        LOG(INFO) << "Skipping unsupported type: " << slot_desc->type().type;
    }
  }
  return Status::OK();
}

// HISTORY
Status IcebergMetadataTableScanner::ReadBooleanValue(JNIEnv* env, Tuple* tuple,
    jobject struct_like_row, SlotDescriptor* slot_desc) {
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

// ENTRIES
Status IcebergMetadataTableScanner::ReadIntValue(JNIEnv* env, Tuple* tuple,
    jobject struct_like_row, SlotDescriptor* slot_desc) {
  jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()],
      iceberg_accessor_get_, struct_like_row);
  RETURN_ERROR_IF_EXC(env);
  if (accessed_value == nullptr) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  jint result = env->CallIntMethod(accessed_value, int_value_);
  RETURN_ERROR_IF_EXC(env);
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  *reinterpret_cast<int64_t*>(slot) = reinterpret_cast<int32_t>(result);
  return Status::OK();
}

// HISTORY
Status IcebergMetadataTableScanner::ReadLongValue(JNIEnv* env, Tuple* tuple,
    jobject struct_like_row, SlotDescriptor* slot_desc) {
  jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()],
      iceberg_accessor_get_, struct_like_row);
  RETURN_ERROR_IF_EXC(env);
  if (accessed_value == nullptr) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  jlong result = env->CallLongMethod(accessed_value, long_value_);
  RETURN_ERROR_IF_EXC(env);
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  *reinterpret_cast<int64_t*>(slot) = reinterpret_cast<int64_t>(result);
  return Status::OK();
}

// HISTORY
Status IcebergMetadataTableScanner::ReadTimeStampValue(JNIEnv* env, Tuple* tuple,
    jobject struct_like_row, SlotDescriptor* slot_desc) {
  jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()],
      iceberg_accessor_get_, struct_like_row);
  RETURN_ERROR_IF_EXC(env);
  if (accessed_value == nullptr) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  jlong result = env->CallLongMethod(accessed_value, long_value_);
  RETURN_ERROR_IF_EXC(env);
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  *reinterpret_cast<TimestampValue*>(slot) = TimestampValue::FromUnixTimeMicros(result, UTCPTR);
  return Status::OK();
}

// SNAPSHOTS
Status IcebergMetadataTableScanner::ReadStringValue(JNIEnv* env, Tuple* tuple,
    jobject struct_like_row, SlotDescriptor* slot_desc, MemPool* tuple_data_pool) {
  jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()],
      iceberg_accessor_get_, struct_like_row);
  RETURN_ERROR_IF_EXC(env);
  if (accessed_value == nullptr) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return Status::OK();
  }
  jobject result = env->CallObjectMethod(accessed_value, char_sequence_value_);
  jstring jstr = (jstring)result;
  JniUtfCharGuard msg_str_guard;
  RETURN_IF_ERROR(JniUtfCharGuard::create(env, jstr, &msg_str_guard));

  LOG(INFO) << "TMATE: " << msg_str_guard.get() << " length: " << strlen(msg_str_guard.get());
  RETURN_ERROR_IF_EXC(env);

  StringValue str;
  str.len = strlen(msg_str_guard.get());
  
  str.ptr = reinterpret_cast<char*>(tuple_data_pool->TryAllocateUnaligned(strlen(msg_str_guard.get())));
  memcpy(str.ptr, msg_str_guard.get(), str.len);

  void* slot = tuple->GetSlot(slot_desc->tuple_offset());
  StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
  *str_slot = str;

  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

}