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

#include "exec/iceberg-metadata/iceberg-metadata-scan-node.h"
#include "exec/exec-node-util.h"
#include "exec/scan-node.h"
#include "exec/text-converter.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/timestamp-value.inline.h"
#include "util/jni-util.h"
#include "common/status.h"
#include "util/bit-util.h"
#include "common/names.h"

using namespace impala;

class Tuple;

Status IcebergMetadataScanPlanNode::CreateExecNode(
    RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new IcebergMetadataScanNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

Status IcebergMetadataScanNode::Init() {
  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }
  return Status::OK();
}

IcebergMetadataScanNode::IcebergMetadataScanNode(ObjectPool* pool,
    const IcebergMetadataScanPlanNode& pnode, const DescriptorTbl& descs)
  : ScanNode(pool, pnode, descs),
    tuple_id_(pnode.tnode_->hdfs_scan_node.tuple_id),
    table_name_(new TTableName(pnode.tnode_->iceberg_scan_metadata_node.table_name)) {
    metadata_table_name_ = new string(pnode.tnode_->iceberg_scan_metadata_node.metadata_table_name.c_str());
};

Status IcebergMetadataScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  RETURN_IF_ERROR(Init());

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  if (tuple_desc_ == NULL) {
    // TODO: make sure we print all available diagnostic output to our error log
    return Status("Failed to get tuple descriptor.");
  }

  iceberg_metadata_scanner_.reset(new IcebergMetadataTableScanner(tuple_desc_,
      metadata_table_name_));
  RETURN_IF_ERROR(iceberg_metadata_scanner_->Init());

  mem_pool_ = new MemPool(mem_tracker());

  return Status::OK();
}

Status IcebergMetadataScanNode::Open(RuntimeState* state) {
  //TODO: once I have decent functionality split the GetNext()
  return Status::OK();
}

Status IcebergMetadataScanNode::GetCatalogTable(JNIEnv* env, jobject* jtable) {
  Frontend* fe = ExecEnv::GetInstance()->frontend();
  RETURN_IF_ERROR(fe->GetCatalogTable(table_name_, jtable));
  return Status::OK();
}

// Indicates whether there are more rows to process. Set in hbase_scanner_.Next().
Status IcebergMetadataScanNode::GetNext(RuntimeState* state, RowBatch* row_batch,
    bool* eos) {
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) {
    return Status("Failed to get/create JVM");
  }

  jobject* jtable = new jobject();
  RETURN_IF_ERROR(GetCatalogTable(env, jtable)); // stays here
  RETURN_IF_ERROR(iceberg_metadata_scanner_->ScanMetadataTable(env, jtable));

  while(true) {
    if (row_batch->AtCapacity() || *eos) {
      return Status::OK();
    }
    RETURN_IF_ERROR(iceberg_metadata_scanner_->GetNext(env, row_batch, state, eos));


  }

  mem_pool_->FreeAll();
  return Status::OK();
}

Status IcebergMetadataScanNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  LOG(INFO) << "QTMATE: row batch reset";
  row_batch->Reset();
  return ExecNode::Reset(state, row_batch);
}

void IcebergMetadataScanNode::Close(RuntimeState* state) {
  LOG(INFO) << "QTMATE: scan close";
  ScanNode::Close(state);
}