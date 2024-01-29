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

#include "exec/iceberg-metadata/iceberg-metadata-scanner.h"
#include "util/jni-util.h"

namespace impala {

Status IcebergMetadataScanner::InitJNI() {
  DCHECK(impala_iceberg_metadata_scanner_cl_ == nullptr) << "InitJNI() already called!";
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  // Global class references:
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/impala/util/IcebergMetadataScanner",
      &impala_iceberg_metadata_scanner_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/Accessor", &iceberg_accessor_cl_));
  // Method ids:
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_accessor_cl_, "get",
      "(Ljava/lang/Object;)Ljava/lang/Object;", &iceberg_accessor_get_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "<init>", "(Lorg/apache/impala/catalog/FeIcebergTable;Ljava/lang/String;)V",
      &iceberg_metadata_scanner_ctor_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "ScanMetadataTable", "()V", &scan_metadata_table_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "GetNext", "()Lorg/apache/iceberg/StructLike;", &get_next_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "GetNextArrayItem", "(Ljava/util/List;Ljava/lang/Class;)Ljava/lang/Object;", &get_next_array_item_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "GetValueByPos", "(Lorg/apache/iceberg/StructLike;ILjava/lang/Class;)Ljava/lang/Object;", &get_value_by_pos_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "GetAccessor", "(I)Lorg/apache/iceberg/Accessor;", &get_accessor_));
  return Status::OK();
}

Status IcebergMetadataScanner::CreateIcebergMetadataScanner(JNIEnv* env, jobject jtable, const char* metadata_table_name) {
  jstring jstr_metadata_table_name = env->NewStringUTF(metadata_table_name);
  jobject jmetadata_scanner = env->NewObject(impala_iceberg_metadata_scanner_cl_,
  iceberg_metadata_scanner_ctor_, jtable, jstr_metadata_table_name);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, jmetadata_scanner, &jmetadata_scanner_));
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}


Status IcebergMetadataScanner::ScanMetadataTable(JNIEnv* env) {
  // SCOPED_TIMER(iceberg_api_scan_timer_);
  google::FlushLogFiles(google::GLOG_INFO);
  env->CallObjectMethod(jmetadata_scanner_, scan_metadata_table_);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergMetadataScanner::CreateAccessorForFieldId(JNIEnv* env, int field_id,
    SlotId slot_id) {
  jobject accessor_for_field = env->CallObjectMethod(jmetadata_scanner_,
      get_accessor_, field_id);
  RETURN_ERROR_IF_EXC(env);
  jobject accessor_for_field_global_ref;
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, accessor_for_field,
      &accessor_for_field_global_ref));
  jaccessors_[slot_id] = accessor_for_field_global_ref;
  LOG(INFO) << "TMATE Adding accessor: field_id: " << field_id << " slot_id: " << slot_id << " accessor ptr: " << accessor_for_field_global_ref;
  return Status::OK();
}


// Let's just access the value here and let the reader raead it
Status IcebergMetadataScanner::AccessValue(JNIEnv* env, SlotDescriptor* slot_desc,
    jobject struct_like_row, jclass clazz, jobject& result) {
  DCHECK(slot_desc != nullptr);

  // get the accessor first
  jobject accessor = GetAccessor(slot_desc->id());
  // we have to choose a strategy, if we have an accessor, let's use that
  if (accessor != nullptr)  {
    // this means we have an accessor available, let's use it
    result = env->CallObjectMethod(accessor, iceberg_accessor_get_,
        struct_like_row);
    RETURN_ERROR_IF_EXC(env);
  } else {
    // we do not have accessor for LIST/MAP elements
    if (slot_desc->parent()->isTupleOfStructSlot()) { // 
      LOG(INFO) << "TMATE: Tuple of a struct, slot;";
      google::FlushLogFiles(google::GLOG_INFO);
      // cannot directly access this value, need positional access:
      // struct_like_row.get(pos, type?)
      int pos = slot_desc->col_path().back();
      switch (slot_desc->type().type) {
        case TYPE_BOOLEAN: { // java.lang.Boolean
          LOG(INFO) << "TMATE: BOOLEAN";
          google::FlushLogFiles(google::GLOG_INFO);
          RETURN_IF_ERROR(GetValueByPos(env, struct_like_row, pos, clazz, result));
          break;
        } case TYPE_STRING: { // java.lang.String
          LOG(INFO) << "TMATE: TYPE_STRING";
          google::FlushLogFiles(google::GLOG_INFO);
          RETURN_IF_ERROR(GetValueByPos(env, struct_like_row, pos, clazz, result));
          break;
        }
        default:
          VLOG(3) << "Skipping unsupported column type: " << slot_desc->type().type;
      }
    } else {
      // We can directly access this value
      result = struct_like_row;
    }
  }

  // it should return the struct like
  
  return Status::OK();
}

jobject IcebergMetadataScanner::GetAccessor(SlotId slot_id) {
  if (jaccessors_.find(slot_id) == jaccessors_.end()) return nullptr;
  else return jaccessors_[slot_id];
}

Status IcebergMetadataScanner::GetNext(JNIEnv* env, jobject& struct_like_row) {
  struct_like_row = env->CallObjectMethod(jmetadata_scanner_, get_next_);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergMetadataScanner::GetNextArrayItem(JNIEnv* env, jobject list, jobject& result) {
  result = env->CallObjectMethod(jmetadata_scanner_,
      get_next_array_item_, list, nullptr);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergMetadataScanner::GetValueByPos(JNIEnv* env, jobject struct_like, int pos,
    jclass clazz, jobject &result) {
  result = env->CallObjectMethod(jmetadata_scanner_, get_value_by_pos_, struct_like, pos, clazz);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

}