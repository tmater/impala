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

#include "exec/iceberg-metadata/iceberg-accessor.h"

namespace impala {

// TODO:
//  - Move all the class references to this class
// 


Status IcebergMetadataScanner::InitJNI() {
  DCHECK(impala_iceberg_metadata_scanner_cl_ == nullptr) << "InitJNI() already called!";
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  // Global class references:
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/Accessor", &iceberg_accessor_cl_));
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env,
      "org/apache/iceberg/StructLike", &iceberg_structlike_cl_));
  // Method ids:
  RETURN_IF_ERROR(JniUtil::GetMethodID(env, iceberg_accessor_cl_, "get",
      "(Ljava/lang/Object;)Ljava/lang/Object;", &iceberg_accessor_get_));

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

}
