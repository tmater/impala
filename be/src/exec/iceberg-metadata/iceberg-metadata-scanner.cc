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
  // Method ids:
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "<init>", "(Lorg/apache/impala/catalog/FeIcebergTable;Ljava/lang/String;)V",
      &iceberg_metadata_scanner_ctor_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "ScanMetadataTable", "()V", &scan_metadata_table_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "GetNext", "()Lorg/apache/iceberg/StructLike;", &get_next_));
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, impala_iceberg_metadata_scanner_cl_,
      "GetAccessor", "(I)Lorg/apache/iceberg/Accessor;", &get_accessor_));
  return Status::OK();
}

Status IcebergMetadataScanner::CreateIcebergMetadataScanner(JNIEnv* env, jobject jtable, const char* metadata_table_name) {
  LOG(INFO) << "TMATE: " << jtable << " ---- " << metadata_table_name;
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
  LOG(INFO) << "TMATE: " << jmetadata_scanner_;
  google::FlushLogFiles(google::GLOG_INFO);
  env->CallObjectMethod(jmetadata_scanner_, scan_metadata_table_);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergMetadataScanner::CreateAccessorForFieldId(JNIEnv* env, int field_id,
    SlotId slot_id) {
  LOG(INFO) << "TMATE " << field_id << " " << slot_id;
  jobject accessor_for_field = env->CallObjectMethod(jmetadata_scanner_,
      get_accessor_, field_id);
  RETURN_ERROR_IF_EXC(env);
  jobject accessor_for_field_global_ref;
  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, accessor_for_field,
      &accessor_for_field_global_ref));
  jaccessors_[slot_id] = accessor_for_field_global_ref;
  return Status::OK();
}

Status AccessValue(JNIEnv* env, SlotId slot_id) {

  return Status::OK();
}

// TBD
jobject IcebergMetadataScanner::GetAccessor(SlotId slot_id) {
  return jaccessors_[slot_id];
}

Status IcebergMetadataScanner::GetNext(JNIEnv* env, jobject& struct_like_row) {
  struct_like_row = env->CallObjectMethod(jmetadata_scanner_, get_next_);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status IcebergMetadataScanner::GetNextArrayItem() {
  

  return Status::OK();
}

}