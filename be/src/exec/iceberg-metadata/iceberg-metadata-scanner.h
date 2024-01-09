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

#pragma once

#include "common/global-types.h"
#include "exec/scan-node.h"

#include <jni.h>

namespace impala {

class RuntimeState;

/// Friend class of the FE IcebergMetadataScanner class.
class IcebergMetadataScanner {

 public:
  /// JNI setup. Creates global references for Java classes and find method ids.
  /// Initializes static members, should be called once per process lifecycle.
  static Status InitJNI() WARN_UNUSED_RESULT;

  Status CreateIcebergMetadataScanner(JNIEnv* env, jobject jtable, const char* metadata_table_name);

  // TODO: method for every IcebergMetadataScanner methods
  Status ScanMetadataTable(JNIEnv* env);

  Status CreateAccessorForFieldId(JNIEnv* env, int field_id, SlotId slot_id);

  jobject GetAccessor(SlotId slot_id);

  Status AccessValue(JNIEnv* env, SlotId slot_id);

  Status GetNext(JNIEnv* env, jobject& struct_like_row);

  Status GetNextArrayItem(JNIEnv* env, jobject list, jobject& result);

 private:
  /// Global class references created with JniUtil.
  inline static jclass impala_iceberg_metadata_scanner_cl_ = nullptr;

  /// Method references created with JniUtil.
  inline static jmethodID iceberg_metadata_scanner_ctor_ = nullptr;
  inline static jmethodID scan_metadata_table_ = nullptr;
  inline static jmethodID get_accessor_ = nullptr;
  inline static jmethodID get_next_ = nullptr;
  inline static jmethodID get_next_array_item_ = nullptr;

  /// Iceberg metadata scanner Java object, it helps preparing the metadata table and
  /// executes an Iceberg table scan. Allows the ScanNode to fetch the metadata from
  /// the Java Heap.
  jobject jmetadata_scanner_;

  /// Accessor map for the scan result, pairs the slot ids with the java Accessor
  /// objects.
  std::unordered_map<SlotId, jobject> jaccessors_;

};


}