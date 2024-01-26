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
#include "exec/iceberg-metadata/iceberg-row-reader.h"
#include "exec/iceberg-metadata/iceberg-metadata-scanner.h"
#include "runtime/collection-value-builder.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/tuple-row.h"
#include "util/jni-util.h"

namespace impala {

IcebergRowReader::IcebergRowReader(IcebergMetadataScanner& metadata_scanner)
  : metadata_scanner_(metadata_scanner) {}

Status IcebergRowReader::InitJNI() {
  DCHECK(iceberg_accessor_cl_ == nullptr) << "InitJNI() already called!";
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  // Global class references:
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
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, list_cl_, "get",
      "(I)Ljava/lang/Object;", &list_get_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, list_cl_, "size",
      "()I", &list_size_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_accessor_cl_, "get",
      "(Ljava/lang/Object;)Ljava/lang/Object;", &iceberg_accessor_get_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_boolean_cl_, "booleanValue", "()Z",
      &boolean_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_int_cl_, "intValue", "()I",
      &int_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_long_cl_, "longValue", "()J",
      &long_value_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, java_char_sequence_cl_, "toString",
      "()Ljava/lang/String;", &char_sequence_to_string_));
  return Status::OK();
}

Status IcebergRowReader::MaterializeTuple(JNIEnv* env,
    jobject struct_like_row, const TupleDescriptor* tuple_desc, Tuple* tuple,
    MemPool* tuple_data_pool, RuntimeState* state) {
  DCHECK(env != nullptr);
  DCHECK(struct_like_row != nullptr);
  DCHECK(tuple != nullptr);
  DCHECK(tuple_data_pool != nullptr);
  DCHECK(tuple_desc != nullptr);

  LOG(INFO) << "TMATE MaterializeTuple tuple_desc: " << tuple_desc->DebugString();
  google::FlushLogFiles(google::GLOG_INFO);

  for (SlotDescriptor* slot_desc: tuple_desc->slots()) {
    LOG(INFO) << "TMATE MaterializeTuple slot_desc: " << slot_desc->DebugString();
    google::FlushLogFiles(google::GLOG_INFO);

    jobject accessor = metadata_scanner_.GetAccessor(slot_desc->id());
    jobject accessed_value;

    LOG(INFO) << "TMATE: accessor: " << accessor;
    google::FlushLogFiles(google::GLOG_INFO);
    if (accessor == nullptr) { 
      // we do not have accessor for LIST/MAP elements
      if (tuple_desc->isTupleOfStructSlot()) {
        LOG(INFO) << "TMATE: Tuple of a struct, slot;";
        google::FlushLogFiles(google::GLOG_INFO);
        // cannot directly access this value, need positional access:
        // struct_like_row.get(pos, type?)
        int pos = slot_desc->col_path().back();
        switch (slot_desc->type().type) {
          case TYPE_BOOLEAN: { // java.lang.Boolean
            LOG(INFO) << "TMATE: BOOLEAN";
            google::FlushLogFiles(google::GLOG_INFO);
            RETURN_IF_ERROR(metadata_scanner_.GetValueByPos(env, struct_like_row, pos, java_boolean_cl_, accessed_value));
            break;
          } case TYPE_STRING: { // java.lang.String
            LOG(INFO) << "TMATE: TYPE_STRING";
            google::FlushLogFiles(google::GLOG_INFO);
            RETURN_IF_ERROR(metadata_scanner_.GetValueByPos(env, struct_like_row, pos, java_char_sequence_cl_, accessed_value));
            break;
          }
          default:
            VLOG(3) << "Skipping unsupported column type: " << slot_desc->type().type;
        }
        // accessed_value = struct_like_row;
      } else {
        // We can directly access this value
        accessed_value = struct_like_row;
      }
    } else {
      accessed_value = env->CallObjectMethod(accessor, iceberg_accessor_get_,
          struct_like_row);
      RETURN_ERROR_IF_EXC(env);
    }
    if (accessed_value == nullptr) {
      tuple->SetNull(slot_desc->null_indicator_offset());
      continue;
    }
    void* slot = tuple->GetSlot(slot_desc->tuple_offset());
    switch (slot_desc->type().type) {
      case TYPE_BOOLEAN: { // java.lang.Boolean
        RETURN_IF_ERROR(WriteBooleanSlot(env, accessed_value, slot));
        break;
      } case TYPE_INT: { // java.lang.Integer
        RETURN_IF_ERROR(WriteIntSlot(env, accessed_value, slot));
        break;
      } case TYPE_BIGINT: { // java.lang.Long
        RETURN_IF_ERROR(WriteLongSlot(env, accessed_value, slot));
        break;
      } case TYPE_TIMESTAMP: { // org.apache.iceberg.types.TimestampType
        RETURN_IF_ERROR(WriteTimeStampSlot(env, accessed_value, slot));
        break;
      } case TYPE_STRING: { // java.lang.String
        RETURN_IF_ERROR(WriteStringSlot(env, accessed_value, slot, tuple_data_pool));
        break;
      } case TYPE_STRUCT: {
        RETURN_IF_ERROR(WriteStructSlot(env, struct_like_row, slot_desc, tuple,
            tuple_data_pool, state));
        break;
      } case TYPE_ARRAY: {
        RETURN_IF_ERROR(WriteArraySlot(env, accessed_value, (CollectionValue*)slot, slot_desc, tuple,
            tuple_data_pool, state));
        break;
      }
      default:
        // Skip the unsupported type and set it to NULL
        tuple->SetNull(slot_desc->null_indicator_offset());
        VLOG(3) << "Skipping unsupported column type: " << slot_desc->type().type;
    }
  }
  return Status::OK();
}

Status IcebergRowReader::WriteBooleanSlot(JNIEnv* env, jobject accessed_value,
    void* slot) {
  DCHECK(accessed_value != nullptr);
  // DCHECK(env->IsInstanceOf(accessed_value, java_boolean_cl_) == JNI_TRUE);
  jboolean result = env->CallBooleanMethod(accessed_value, boolean_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<bool*>(slot) = (bool)(result == JNI_TRUE);
  return Status::OK();
}

Status IcebergRowReader::WriteIntSlot(JNIEnv* env, jobject accessed_value, void* slot) {
  DCHECK(accessed_value != nullptr);
  DCHECK(env->IsInstanceOf(accessed_value, java_int_cl_) == JNI_TRUE);
  jint result = env->CallIntMethod(accessed_value, int_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<int32_t*>(slot) = reinterpret_cast<int32_t>(result);
  return Status::OK();
}

Status IcebergRowReader::WriteLongSlot(JNIEnv* env, jobject accessed_value, void* slot) {
  DCHECK(accessed_value != nullptr);
  DCHECK(env->IsInstanceOf(accessed_value, java_long_cl_) == JNI_TRUE);
  jlong result = env->CallLongMethod(accessed_value, long_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<int64_t*>(slot) = reinterpret_cast<int64_t>(result);
  return Status::OK();
}

Status IcebergRowReader::WriteTimeStampSlot(JNIEnv* env, jobject accessed_value,
    void* slot) {
  DCHECK(accessed_value != nullptr);
  DCHECK(env->IsInstanceOf(accessed_value, java_long_cl_) == JNI_TRUE);
  jlong result = env->CallLongMethod(accessed_value, long_value_);
  RETURN_ERROR_IF_EXC(env);
  *reinterpret_cast<TimestampValue*>(slot) = TimestampValue::FromUnixTimeMicros(result,
      UTCPTR);
  return Status::OK();
}

Status IcebergRowReader::WriteStringSlot(JNIEnv* env, jobject accessed_value, void* slot,
      MemPool* tuple_data_pool) {
  DCHECK(accessed_value != nullptr);
  // DCHECK(env->IsInstanceOf(accessed_value, java_char_sequence_cl_) == JNI_TRUE);
  jstring result = static_cast<jstring>(env->CallObjectMethod(accessed_value,
      char_sequence_to_string_));
  RETURN_ERROR_IF_EXC(env);
  JniUtfCharGuard str_guard;
  RETURN_IF_ERROR(JniUtfCharGuard::create(env, result, &str_guard));
  // Allocate memory and copy the string from the JVM to the RowBatch
  int str_len = strlen(str_guard.get());
  char* buffer = reinterpret_cast<char*>(tuple_data_pool->TryAllocateUnaligned(str_len));
  if (UNLIKELY(buffer == nullptr)) {
    string details = strings::Substitute("Failed to allocate $1 bytes for string.",
        str_len);
    return tuple_data_pool->mem_tracker()->MemLimitExceeded(nullptr, details, str_len);
  }
  memcpy(buffer, str_guard.get(), str_len);
  reinterpret_cast<StringValue*>(slot)->Assign(buffer, str_len);
  return Status::OK();
}

Status IcebergRowReader::WriteStructSlot(JNIEnv* env, jobject struct_like_row,
    SlotDescriptor* slot_desc, Tuple* tuple, MemPool* tuple_data_pool, RuntimeState* state) {
  DCHECK(slot_desc != nullptr);
  RETURN_IF_ERROR(MaterializeTuple(env, struct_like_row,
      slot_desc->children_tuple_descriptor(), tuple, tuple_data_pool, state));
  return Status::OK();
}

Status IcebergRowReader::WriteArraySlot(JNIEnv* env, jobject struct_like_row,
    CollectionValue* slot, SlotDescriptor* slot_desc, Tuple* tuple,
    MemPool* tuple_data_pool, RuntimeState* state) {
  LOG(INFO) << "TMATE Writing Array slot children tuple descriptor: "
      << slot_desc->children_tuple_descriptor()->DebugString();
  DCHECK(env->IsInstanceOf(struct_like_row, list_cl_) == JNI_TRUE);
  const TupleDescriptor* item_desc = slot_desc->children_tuple_descriptor();

  // TupleRow* tuple_row_mem = nullptr;
  // Recursively read the collection into a new CollectionValue.
  *slot = CollectionValue();
  CollectionValueBuilder coll_value_builder(
      slot, *slot_desc->children_tuple_descriptor(), tuple_data_pool, state);

  // Need to get memory for the collection
  Tuple* tuple_mem;
  int num_tuples = env->CallIntMethod(struct_like_row, list_size_);
  RETURN_ERROR_IF_EXC(env);
  tuple_data_pool = coll_value_builder.pool();
  RETURN_IF_ERROR(coll_value_builder.GetFreeMemory(&tuple_mem, &num_tuples));

  jobject element;
  int elementCntr = 0;
  while (true) {
    // need to re-init the tuple
    // we can get a size of array to be more precise
    RETURN_IF_ERROR(metadata_scanner_.GetNextArrayItem(env, struct_like_row, element));
    LOG(INFO) << "TMATE: Before element! Children tuple: " << slot_desc->children_tuple_descriptor()->DebugString();
    if (element == nullptr) break;
    LOG(INFO) << "TMATE: Could call materialize view!";
    RETURN_IF_ERROR(MaterializeTuple(env, element,
        slot_desc->children_tuple_descriptor(), tuple_mem, tuple_data_pool, state));
    elementCntr++;
    tuple_mem += item_desc->byte_size();
  }
  coll_value_builder.CommitTuples(elementCntr);
  // Pass the array to a 

  // the call MaterializeTuple, with iterated struct_like_row
  // bool continue_execution =
  //     parent_->AssembleCollection(children_, new_collection_rep_level(), &builder);
  // if (!continue_execution) return false;
  return Status::OK();


}

}