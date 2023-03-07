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

#ifndef ICEBERG_METADATA_TABLE_SCANNER_H
#define ICEBERG_METADATA_TABLE_SCANNER_H

#include "runtime/exec-env.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "service/frontend.h"
#include "util/jni-util.h"

using namespace impala;

namespace impala {

/// JNI wrapper class implementing minimal functionality for scanning an Iceberg Metadata
/// table.
///
/// Iceberg version compatibility: 0.14
/// 
/// 
class IcebergMetadataTableScanner {
 public:
  /// Initialize the tuple descriptor and the metadtata_table_name.
  IcebergMetadataTableScanner(const TupleDescriptor* tuple_desc,
      const string* metadatat_table_name);

  /// JNI setup. Create global references to classes, and find method ids.
  Status Init();

  /// Creates the Java Iceberg API Metadata table object and executes the table scan.
  /// It also initializes the Accessors.
  Status ScanMetadataTable(JNIEnv* env, jobject* fe_table);

  /// Iterates over the scan result and fills the RowBatch till it reaches its limit.
  Status GetNext(JNIEnv* env, RowBatch* row_batch, RuntimeState* state, bool* eos);

 private:
  /// Global class references created with JniUtil.
  static jclass fe_iceberg_table_cl_;  // org.apache.impala.catalog.FeIcebergTable
  static jclass iceberg_api_table_cl_; // org.apache.iceberg.Table
  static jclass iceberg_metadata_table_utils_cl_;
  static jclass iceberg_table_scan_cl_;
  static jclass iceberg_metadatatable_type_cl_;
  static jclass iceberg_scan_cl_;
  static jclass iceberg_file_scan_task_cl_;
  static jclass iceberg_closeable_iterable_cl_;
  static jclass iceberg_closeable_iterator_cl_;
  static jclass iceberg_data_task_cl_;
  static jclass iceberg_struct_like_cl_;
  static jclass iceberg_accessor_cl_;
  static jclass iceberg_schema_cl_;
  static jclass iceberg_nested_field_cl_;
  static jclass list_cl_;

  /// Method references references created with JniUtil.
  static jmethodID fe_iceberg_table_get_iceberg_api_table_;
  static jmethodID iceberg_table_new_scan_;
  static jmethodID iceberg_metadata_table_utils_create_metadata_table_instance_;
  static jmethodID iceberg_table_scan_plan_files_;
  static jmethodID iceberg_closable_iterable_iterator_;
  static jmethodID iceberg_closeable_iterator_has_next_;
  static jmethodID iceberg_closeable_iterator_next_;
  static jmethodID iceberg_data_task_rows_;
  static jmethodID iceberg_table_schema_;
  static jmethodID iceberg_schema_columns_;
  static jmethodID iceberg_nested_field_field_id_;
  static jmethodID iceberg_schema_accessor_for_field_;
  static jmethodID iceberg_accessor_get_;
  static jmethodID list_get_;

  /// Getting enum objects are not straigthforward calls:
  /// enum -> field -> object
  Status CreateJIcebergMetadataTable(JNIEnv* env, jobject* jtable);
 
  /// Create the accessors for the StructLike rows.
  Status CreateJAccessors(JNIEnv* env, jobject* jtable);

  /// Takes a StructLike object accesses its records, create a tuple from the result and
  /// adds it to the row_batch.
  Status AddRow(JNIEnv* env, jobject struct_like_row, RowBatch* row_batch,
      RuntimeState* state);

 protected:
  /// Descriptor of tuples read from Iceberg metadata table.
  const TupleDescriptor* tuple_desc_;

  /// The name of the metadata table which has to match the MetadataTableType enum.
  /// The validity of this name should be done by the frontend.
  const string* metadata_table_name_;

  /// Iceberg metadata table object that is created by this scanner.
  jobject* jmetadata_table_ = new jobject();

  /// Accessor object for the scan result. These are in the same order as the slot
  /// descriptors.
  /// TODO: use map
  std::map<int, jobject> jaccessors_;

  /// Java iterators to iterate thorugh the table scan result.
  jobject file_scan_task_iterable_;
  jobject file_scan_task_iterator_;

};

}

#endif