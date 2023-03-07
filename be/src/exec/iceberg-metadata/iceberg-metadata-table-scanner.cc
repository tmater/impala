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

jclass IcebergMetadataTableScanner::fe_iceberg_table_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_api_table_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_metadata_table_utils_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_table_scan_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_metadatatable_type_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_scan_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_file_scan_task_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_data_task_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_closeable_iterable_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_closeable_iterator_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_struct_like_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_accessor_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_schema_cl_ = NULL;
jclass IcebergMetadataTableScanner::iceberg_nested_field_cl_ = NULL;
jclass IcebergMetadataTableScanner::list_cl_ = NULL;

jmethodID IcebergMetadataTableScanner::fe_iceberg_table_get_iceberg_api_table_ = NULL;
jmethodID IcebergMetadataTableScanner::iceberg_table_new_scan_ = NULL;
jmethodID IcebergMetadataTableScanner::
    iceberg_metadata_table_utils_create_metadata_table_instance_ = NULL;
jmethodID IcebergMetadataTableScanner::iceberg_table_scan_plan_files_ = NULL;
jmethodID IcebergMetadataTableScanner::iceberg_closable_iterable_iterator_ = NULL;
jmethodID IcebergMetadataTableScanner::iceberg_closeable_iterator_has_next_ = NULL;
jmethodID IcebergMetadataTableScanner::iceberg_closeable_iterator_next_ = NULL;
jmethodID IcebergMetadataTableScanner::iceberg_data_task_rows_ = NULL;
jmethodID IcebergMetadataTableScanner::iceberg_table_schema_ = NULL;
jmethodID IcebergMetadataTableScanner::iceberg_schema_columns_ = NULL;
jmethodID IcebergMetadataTableScanner::iceberg_nested_field_field_id_ = NULL;
jmethodID IcebergMetadataTableScanner::iceberg_schema_accessor_for_field_ = NULL;
jmethodID IcebergMetadataTableScanner::iceberg_accessor_get_ = NULL;
jmethodID IcebergMetadataTableScanner::list_get_ = NULL;

namespace impala {

IcebergMetadataTableScanner::IcebergMetadataTableScanner(
    const TupleDescriptor* tuple_desc,
    const string* metadata_table_name)
  : tuple_desc_(tuple_desc),
    metadata_table_name_(metadata_table_name) {}

Status IcebergMetadataTableScanner::Init() {
  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }
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
      "org/apache/iceberg/MetadataTableType", &iceberg_metadatatable_type_cl_));
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
  return Status::OK();
}

Status IcebergMetadataTableScanner::ScanMetadataTable(JNIEnv* env, jobject* fe_table) {
  RETURN_IF_ERROR(CreateJIcebergMetadataTable(env, fe_table));
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
  RETURN_IF_ERROR(CreateJAccessors(env, jmetadata_table_));
  return Status::OK();
}

Status IcebergMetadataTableScanner::CreateJIcebergMetadataTable(JNIEnv* env,
    jobject* jtable) {
  DCHECK(jtable != NULL);
  jobject jiceberg_api_table = env->CallObjectMethod(*jtable,
      fe_iceberg_table_get_iceberg_api_table_);
  jfieldID metadatatable_type_field = env->GetStaticFieldID(
      iceberg_metadatatable_type_cl_, metadata_table_name_->c_str(),
      "Lorg/apache/iceberg/MetadataTableType;");
  jobject metadatatable_type_object = env->GetStaticObjectField(
      iceberg_metadatatable_type_cl_, metadatatable_type_field);
  jobject metadata_table = env->CallStaticObjectMethod(iceberg_metadatatable_type_cl_,
      iceberg_metadata_table_utils_create_metadata_table_instance_, jiceberg_api_table,
      metadatatable_type_object);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, metadata_table, jmetadata_table_));
  return Status::OK();
}

Status IcebergMetadataTableScanner::CreateJAccessors(JNIEnv* env, jobject* jtable) {
  DCHECK(jtable != NULL);
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
    while(env->CallBooleanMethod(data_rows_iterator,
        iceberg_closeable_iterator_has_next_)) {
      RETURN_ERROR_IF_EXC(env);
      if (row_batch->AtCapacity()) {
        return Status::OK();
      }
      jobject struct_like_row = env->CallObjectMethod(data_rows_iterator,
          iceberg_closeable_iterator_next_);
      RETURN_IF_ERROR(AddRow(env, struct_like_row, row_batch, state));
    }
  }
  *eos = true;
  return Status::OK();
}

Status IcebergMetadataTableScanner::AddRow(JNIEnv* env, jobject struct_like_row,
    RowBatch* row_batch, RuntimeState* state) {
  int64_t tuple_buffer_size;
  uint8_t* tuple_buffer;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buffer_size, &tuple_buffer));
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
  tuple->Init(tuple_buffer_size);

  int tuple_idx;
  int row_idx = row_batch->AddRow();
  TupleRow* row = row_batch->GetRow(row_idx);
  row->SetTuple(tuple_idx, tuple);

  int i = 0;
  for (SlotDescriptor* slot_desc: tuple_desc_->slots()) {
    LOG(INFO) << "slot type: " << slot_desc->type().type << " accessor_id: " << i;
    switch (slot_desc->type().type) {
      case TYPE_BIGINT: {
        jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()], iceberg_accessor_get_, struct_like_row);
        RETURN_ERROR_IF_EXC(env);
        if (accessed_value == nullptr){
          tuple->SetNull(slot_desc->null_indicator_offset());
          i++;
          continue;
        }
        jclass accessed_value_cls = env->GetObjectClass(accessed_value);
        jmethodID longValue = env->GetMethodID(accessed_value_cls, "longValue", "()J");
        jlong resu2lt = env->CallLongMethod(accessed_value, longValue);
        void* slot = tuple->GetSlot(slot_desc->tuple_offset());
        *reinterpret_cast<int64_t*>(slot) = reinterpret_cast<int64_t>(resu2lt);
        break;
      } case TYPE_BOOLEAN: {
        jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()], iceberg_accessor_get_, struct_like_row);
        jclass accessed_value_cls = env->GetObjectClass(accessed_value);
        jmethodID boolValue = env->GetMethodID(accessed_value_cls, "booleanValue", "()Z");
        jboolean resu2lt = env->CallBooleanMethod(accessed_value, boolValue);
        void* slot = tuple->GetSlot(slot_desc->tuple_offset());
        *reinterpret_cast<uint8_t*>(slot) = reinterpret_cast<uint8_t>(resu2lt);
        break;
      } case TYPE_TIMESTAMP: {
        jobject accessed_value = env->CallObjectMethod(jaccessors_[slot_desc->col_pos()], iceberg_accessor_get_, struct_like_row);
        if (accessed_value == nullptr){
          tuple->SetNull(slot_desc->null_indicator_offset());
          i++;
          continue;
        }
        jclass accessed_value_cls = env->GetObjectClass(accessed_value);
        jmethodID longValue = env->GetMethodID(accessed_value_cls, "longValue", "()J");
        jlong resu2lt = env->CallLongMethod(accessed_value, longValue);
        void* slot = tuple->GetSlot(slot_desc->tuple_offset());
        *reinterpret_cast<TimestampValue*>(slot) = TimestampValue::FromUnixTimeMicros(resu2lt, UTCPTR);
        break;
      }
      default:
        LOG(INFO) << "Skipping unsupported type: " << slot_desc->type().type;
    }
    i++;
  }
  row_batch->CommitLastRow();
  return Status::OK();
}

}