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

#ifndef ICEBERG_METADATA_SCAN_NODE_H
#define ICEBERG_METADATA_SCAN_NODE_H

#include "exec/exec-node.h"
#include "exec/exec-node.inline.h"
#include "exec/scan-node.h"
#include "exec/iceberg-metadata/iceberg-metadata-table-scanner.h"
#include <boost/scoped_ptr.hpp>
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"

namespace impala {

class ScalarExpr;
class ScalarExprEvaluator;
class ExecNode;
class MemPool;
class MemTracker;

/// This scanner is intended to be executed on a coordinator host which has already loaded
/// the Iceberg table.
class IcebergMetadataScanPlanNode : public ScanPlanNode {
 public:
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  ~IcebergMetadataScanPlanNode(){}
};

/// ScanNode ancestor -> ExecNode
class IcebergMetadataScanNode : public ScanNode {
 public:
  IcebergMetadataScanNode(ObjectPool* pool, const IcebergMetadataScanPlanNode& pnode,
      const DescriptorTbl& descs);

  virtual Status Prepare(RuntimeState* state) override;

  /// Open the Iceberg TableScan
  virtual Status Open(RuntimeState* state) override;

  /// Get next rowbatch from the table scanner
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

  /// Close the Iceberg TableScan
  virtual void Close(RuntimeState* state) override;

  Status GetCatalogTable(JNIEnv* env, jobject* jtable);

  const TupleDescriptor* tuple_desc() const { return tuple_desc_; }

 protected:
  /// Tuple id resolved in Prepare() to set tuple_desc_;
  TupleId tuple_id_;


  /// Descriptor of tuples read from Iceberg metadata table.
  const TupleDescriptor* tuple_desc_ = nullptr;

  /// Table and metadtata table name.
  const TTableName* table_name_;
  const string* metadata_table_name_;

 private:
  /// Jni helper for scanning an Iceberg metadata table.
  boost::scoped_ptr<IcebergMetadataTableScanner> iceberg_metadata_scanner_;

};

}

#endif
