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

#include <memory>

#include "runtime/exec-env.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "service/frontend.h"
#include "util/jni-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "exec/exec-node.h"
#include "exec/exec-node.inline.h"
#include "exec/scan-node.h"

namespace impala {

class MemPool;
class MemTracker;

/// JNI wrapper class implementing functionality to scan an Iceberg Metadata table.
///
/// Iceberg API provides {createMetadataTableInstance} to create a metadata table out of
/// an {org.apache.iceberg.Table} object. This table can be scanned through Iceberg API's
/// {TableScan} class. The result of the scan is the rows, the fields can be accessed by
/// iterating over the rows and the fields and extracting the values with  {Accessor}(s).
/// For clarity, the matching Java calls are indicated over the JNI calls in comments.
///
/// Requirements:
/// - The Iceberg table has to be loaded in the JVM
/// - Iceberg version compatibility: 1.1
class IcebergMetadataTableScanner {
 public:
  /// Initialize the tuple descriptor and the metadata_table_name.
  IcebergMetadataTableScanner(const TupleDescriptor* tuple_desc,
      const string* metadata_table_name, std::vector<ScalarExpr*> conjuncts,
      std::vector<ScalarExprEvaluator*> conjunct_evals_);

  /// JNI setup. Create global references to classes, and find method ids.
  Status Init(JNIEnv* env);

  /// Create Metadata Table object and the Accessors.
  Status Prepare(JNIEnv* env, jobject* fe_table);

  /// Creates the Java Iceberg API Metadata table object and executes the table scan.
  /// It also initializes the Accessors.
  Status ScanMetadataTable(JNIEnv* env);

  /// Iterates over the scan result and fills the RowBatch till it reaches its limit.
  Status GetNext(JNIEnv* env, RowBatch* row_batch, RuntimeState* state, bool* eos);

 private:
  /// Global class references created with JniUtil.
  inline static jclass fe_iceberg_table_cl_ = NULL;  // org.apache.impala.catalog.FeIcebergTable
  inline static jclass iceberg_api_table_cl_; // org.apache.iceberg.Table
  inline static jclass iceberg_metadata_table_utils_cl_;
  inline static jclass iceberg_table_scan_cl_;
  inline static jclass iceberg_metadata_table_type_cl_;
  inline static jclass iceberg_scan_cl_;
  inline static jclass iceberg_file_scan_task_cl_;
  inline static jclass iceberg_closeable_iterable_cl_;
  inline static jclass iceberg_closeable_iterator_cl_;
  inline static jclass iceberg_data_task_cl_;
  inline static jclass iceberg_struct_like_cl_;
  inline static jclass iceberg_accessor_cl_;
  inline static jclass iceberg_schema_cl_;
  inline static jclass iceberg_nested_field_cl_;
  inline static jclass list_cl_;
  inline static jclass java_boolean_cl_;
  inline static jclass java_int_cl_;
  inline static jclass java_long_cl_;
  inline static jclass java_char_sequence_cl_;

  /// Method references references created with JniUtil.
  inline static jmethodID fe_iceberg_table_get_iceberg_api_table_;
  inline static jmethodID iceberg_table_new_scan_;
  inline static jmethodID iceberg_metadata_table_utils_create_metadata_table_instance_;
  inline static jmethodID iceberg_table_scan_plan_files_;
  inline static jmethodID iceberg_closable_iterable_iterator_;
  inline static jmethodID iceberg_closeable_iterator_has_next_;
  inline static jmethodID iceberg_closeable_iterator_next_;
  inline static jmethodID iceberg_data_task_rows_;
  inline static jmethodID iceberg_table_schema_;
  inline static jmethodID iceberg_schema_columns_;
  inline static jmethodID iceberg_nested_field_field_id_;
  inline static jmethodID iceberg_schema_accessor_for_field_;
  inline static jmethodID iceberg_accessor_get_;
  inline static jmethodID list_get_;
  inline static jmethodID boolean_value_;
  inline static jmethodID int_value_;
  inline static jmethodID long_value_;
  inline static jmethodID char_sequence_value_;

  /// Getting enum objects are not straigthforward calls:
  /// enum -> field -> object
  Status CreateJIcebergMetadataTable(JNIEnv* env, jobject* jtable);
 
  /// Create the accessors for the StructLike rows.
  Status CreateJAccessors(JNIEnv* env);

  /// Takes a StructLike object accesses its records, create a tuple from the result and
  /// adds it to the row_batch.
  Status GetRow(JNIEnv* env, jobject struct_like_row, Tuple* tuple,
      RuntimeState* state, MemPool* tuple_data_pool);

 Status ReadBooleanValue(JNIEnv* env, Tuple* tuple, jobject struct_like_row, SlotDescriptor* slot_desc);
 Status ReadIntValue(JNIEnv* env, Tuple* tuple, jobject struct_like_row, SlotDescriptor* slot_desc);
 Status ReadLongValue(JNIEnv* env, Tuple* tuple, jobject struct_like_row, SlotDescriptor* slot_desc);
 Status ReadTimeStampValue(JNIEnv* env, Tuple* tuple, jobject struct_like_row, SlotDescriptor* slot_desc);
 Status ReadStringValue(JNIEnv* env, Tuple* tuple, jobject struct_like_row, SlotDescriptor* slot_desc, MemPool* tuple_data_pool);

 protected:
  /// Descriptor of tuples read from Iceberg metadata table.
  const TupleDescriptor* tuple_desc_;

  /// The name of the metadata table which has to match the MetadataTableType enum.
  /// The validity of this name should be done by the frontend.
  const string* metadata_table_name_;

  /// Iceberg metadata table object that is created by this scanner.
  jobject* jmetadata_table_ = new jobject();

  std::vector<ScalarExpr*> conjuncts_;
  std::vector<ScalarExprEvaluator*> conjunct_evals_;

  /// Accessor object for the scan result. These are in the same order as the slot
  std::unordered_map<int, jobject> jaccessors_;

  /// Java iterators to iterate thorugh the table scan result.
  jobject file_scan_task_iterable_;
  jobject file_scan_task_iterator_;
  jobject data_rows_iterator_;
  jobject next_struct_like_row_;

  /// Tuple index in tuple row.
  int tuple_idx_;

};

}

#endif