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

#include "exec/scan-node.h"
#include "runtime/runtime-state.h"
#include "util/jni-util.h"

#include <memory>
#include <unordered_map>

namespace impala {

class MemPool;
class MemTracker;
class Status;
class IcebergMetadataScanNode;

/// JNI wrapper class implementing functionality to scan an Iceberg Metadata table.
///
/// Iceberg API provides {createMetadataTableInstance} to create a metadata table out of
/// an {org.apache.iceberg.Table} object. This table can be scanned through Iceberg API's
/// {TableScan} class. The result of the scan is the rows, the fields can be accessed by
/// iterating over the rows and the fields and extracting the values with  {Accessor}(s).
/// For clarity, the matching Java calls are indicated over the JNI calls in comments.
///
/// Primitive Iceberg types are defined as primitive Java types and can be accessed
/// through the java.lang class objects.
///
/// Notes:
/// - The Iceberg table has to be loaded in the JVM.
/// - The Java calls can be seen above the JNI calls in comments.
class IcebergMetadataTableScanner {
 public:
  /// Initialize the tuple descriptor and the metadata_table_name.
  IcebergMetadataTableScanner(IcebergMetadataScanNode* scan_node, RuntimeState* state,
      const TupleDescriptor* tuple_desc, const string* metadata_table_name);

  /// JNI setup. Create global references for Java classes and find method ids.
  Status Init(JNIEnv* env);

  /// Create Metadata Table object and the Accessors.
  Status Prepare(JNIEnv* env, jobject* fe_table);

  /// Creates the Java Iceberg API Metadata table object and executes the table scan.
  Status ScanMetadataTable(JNIEnv* env);

  /// Iterates over the scan result and fills the RowBatch till it reaches its limit.
  Status GetNext(JNIEnv* env, RowBatch* row_batch, RuntimeState* state, bool* eos);

 private:
  /// The enclosing IcebergMetadataTableScanNode.
  IcebergMetadataScanNode* scan_node_;
  RuntimeState* state_;

  /// Global class references created with JniUtil.
  inline static jclass fe_iceberg_table_cl_ = nullptr;
  inline static jclass iceberg_api_table_cl_ = nullptr;
  inline static jclass iceberg_metadata_table_utils_cl_ = nullptr;
  inline static jclass iceberg_table_scan_cl_ = nullptr;
  inline static jclass iceberg_metadata_table_type_cl_ = nullptr;
  inline static jclass iceberg_scan_cl_ = nullptr;
  inline static jclass iceberg_file_scan_task_cl_ = nullptr;
  inline static jclass iceberg_closeable_iterable_cl_ = nullptr;
  inline static jclass iceberg_closeable_iterator_cl_ = nullptr;
  inline static jclass iceberg_data_task_cl_ = nullptr;
  inline static jclass iceberg_struct_like_cl_ = nullptr;
  inline static jclass iceberg_accessor_cl_ = nullptr;
  inline static jclass iceberg_schema_cl_ = nullptr;
  inline static jclass iceberg_nested_field_cl_ = nullptr;
  inline static jclass list_cl_ = nullptr;
  inline static jclass java_boolean_cl_ = nullptr;
  inline static jclass java_int_cl_ = nullptr;
  inline static jclass java_long_cl_ = nullptr;
  inline static jclass java_char_sequence_cl_ = nullptr;

  /// Method references references created with JniUtil.
  inline static jmethodID fe_iceberg_table_get_iceberg_api_table_ = nullptr;
  inline static jmethodID iceberg_table_new_scan_ = nullptr;
  inline static jmethodID iceberg_metadata_table_utils_create_metadata_table_instance_ =
      nullptr;
  inline static jmethodID iceberg_table_scan_plan_files_ = nullptr;
  inline static jmethodID iceberg_closable_iterable_iterator_ = nullptr;
  inline static jmethodID iceberg_closeable_iterator_has_next_ = nullptr;
  inline static jmethodID iceberg_closeable_iterator_next_ = nullptr;
  inline static jmethodID iceberg_data_task_rows_ = nullptr;
  inline static jmethodID iceberg_table_schema_ = nullptr;
  inline static jmethodID iceberg_schema_columns_ = nullptr;
  inline static jmethodID iceberg_nested_field_field_id_ = nullptr;
  inline static jmethodID iceberg_schema_accessor_for_field_ = nullptr;
  inline static jmethodID iceberg_accessor_get_ = nullptr;
  inline static jmethodID list_get_ = nullptr;
  inline static jmethodID boolean_value_ = nullptr;
  inline static jmethodID int_value_ = nullptr;
  inline static jmethodID long_value_ = nullptr;
  inline static jmethodID char_sequence_value_ = nullptr;

  /// Getting enum objects are not straigthforward calls: enum -> field -> object
  Status CreateJIcebergMetadataTable(JNIEnv* env, jobject* jtable);
 
  /// Create the accessors for the StructLike rows.
  Status CreateJAccessors(JNIEnv* env);

  /// Takes a StructLike object accesses its records, create a tuple from the result and
  /// adds it to the row_batch.
  Status MaterializeNextRow(JNIEnv* env, jobject struct_like_row, Tuple* tuple,
      RuntimeState* state, MemPool* tuple_data_pool);

  /// Reads the value of a primitive from the StructLikeRow, translates it to a matching
  /// Impala type and writes it into the target tuple. The related Accessor objects are
  /// stored in the jaccessors_ map and created during Prepare.
  Status ReadBooleanValue(JNIEnv* env, SlotDescriptor* slot_desc, jobject struct_like_row,
      Tuple* tuple);
  Status ReadIntValue(JNIEnv* env, SlotDescriptor* slot_desc, jobject struct_like_row,
      Tuple* tuple);
  Status ReadLongValue(JNIEnv* env, SlotDescriptor* slot_desc, jobject struct_like_row,
      Tuple* tuple);
  /// Iceberg TimeStamp is parsed into TimestampValue.
  Status ReadTimeStampValue(JNIEnv* env, SlotDescriptor* slot_desc,
      jobject struct_like_row, Tuple* tuple);
  /// To obtain a character sequence from JNI the JniUtfCharGuard class is used. Then the
  /// data has to be copied to the tuple_data_pool, because the JVM releases the reference
  /// and reclaims the memory area.
  Status ReadStringValue(JNIEnv* env, SlotDescriptor* slot_desc, jobject struct_like_row,
      Tuple* tuple, MemPool* tuple_data_pool);

 protected:
  /// TupleDescriptor received from the ScanNode.
  const TupleDescriptor* tuple_desc_;

  /// The name of the metadata table which has to match the MetadataTableType enum.
  /// The validity of this name should be veified by the frontend.
  const string* metadata_table_name_;

  /// Iceberg metadata table object that is created by this scanner.
  jobject* jmetadata_table_ = new jobject();

  /// Accessor object for the scan result. These are in the same order as the slot
  std::unordered_map<int, jobject> jaccessors_;

  /// Java iterators to iterate thorugh the table scan result.
  jobject file_scan_task_iterable_;
  jobject file_scan_task_iterator_;
  jobject data_rows_iterator_;
  jobject next_struct_like_row_;

  /// Tuple index in tuple row.
  int tuple_idx_;

  /// Iceberg metadata scan specific counters
  RuntimeProfile::Counter* scan_prepare_timer_;
  RuntimeProfile::Counter* iceberg_api_scan_timer_;
};

}
