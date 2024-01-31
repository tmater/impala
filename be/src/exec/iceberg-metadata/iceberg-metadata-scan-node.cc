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
#include "exec/exec-node.inline.h"
#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "service/frontend.h"
#include "util/jni-util.h"

using namespace impala;

Status IcebergMetadataScanPlanNode::CreateExecNode(
    RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new IcebergMetadataScanNode(pool, *this, state->desc_tbl()));
  return Status::OK();
}

IcebergMetadataScanNode::IcebergMetadataScanNode(ObjectPool* pool,
    const IcebergMetadataScanPlanNode& pnode, const DescriptorTbl& descs)
  : ScanNode(pool, pnode, descs),
    tuple_id_(pnode.tnode_->iceberg_scan_metadata_node.tuple_id),
    table_name_(pnode.tnode_->iceberg_scan_metadata_node.table_name),
    metadata_table_name_(pnode.tnode_->iceberg_scan_metadata_node.metadata_table_name) {}

Status IcebergMetadataScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));
  scan_prepare_timer_ = ADD_TIMER(runtime_profile(), "ScanPrepareTime");
  iceberg_api_scan_timer_ = ADD_TIMER(runtime_profile(), "IcebergApiScanTime");
  SCOPED_TIMER(scan_prepare_timer_);
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  if (tuple_desc_ == nullptr) {
    return Status("Failed to get tuple descriptor, tuple id: " +
        std::to_string(tuple_id_));
  }
  // Get the FeTable object from the Frontend
  jobject jtable;
  RETURN_IF_ERROR(GetCatalogTable(&jtable));
  // Create the Java Scanner object and scan the table
  RETURN_ERROR_IF_EXC(env);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(metadata_scanner_.CreateIcebergMetadataScanner(env, jtable, metadata_table_name_.c_str()));
  RETURN_IF_ERROR(metadata_scanner_.ScanMetadataTable(env));
  RETURN_IF_ERROR(metadata_scanner_.InitSlotIdFieldIdMap(tuple_desc_));
  return Status::OK();
}

Status IcebergMetadataScanNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Open(state));
  iceberg_row_reader_.reset(new IcebergRowReader(metadata_scanner_));
  return Status::OK();
}

Status IcebergMetadataScanNode::GetNext(RuntimeState* state, RowBatch* row_batch,
    bool* eos) {
  RETURN_IF_CANCELLED(state);
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("Failed to get/create JVM");
  SCOPED_TIMER(materialize_tuple_timer());
  // Allocate buffer for RowBatch and init the tuple
  uint8_t* tuple_buffer;
  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(row_batch->ResizeAndAllocateTupleBuffer(state, &tuple_buffer_size,
      &tuple_buffer));
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
  tuple->Init(tuple_buffer_size);
  while (!row_batch->AtCapacity()) {
    int row_idx = row_batch->AddRow();
    TupleRow* tuple_row = row_batch->GetRow(row_idx);
    tuple_row->SetTuple(0, tuple);

    // Get the next row from 'org.apache.impala.util.IcebergMetadataScanner'
    jobject struct_like_row;
    RETURN_IF_ERROR(metadata_scanner_.GetNext(env, struct_like_row));
    // jobject struct_like_row = env->CallObjectMethod(jmetadata_scanner_, get_next_);
    RETURN_ERROR_IF_EXC(env);
    // When 'struct_like_row' is null, there are no more rows to read
    if (struct_like_row == nullptr) {
      *eos = true;
      return Status::OK();
    } 
    // Translate a StructLikeRow from Iceberg to Tuple
    RETURN_IF_ERROR(iceberg_row_reader_->MaterializeTuple(env, struct_like_row,
        tuple_desc_, tuple, row_batch->tuple_data_pool(), state));
    // TODO: materialize collection type
    env->DeleteLocalRef(struct_like_row);
    RETURN_ERROR_IF_EXC(env);
    COUNTER_ADD(rows_read_counter(), 1);

    // Evaluate conjuncts on this tuple row
    if (ExecNode::EvalConjuncts(conjunct_evals().data(),
        conjunct_evals().size(), tuple_row)) {
      row_batch->CommitLastRow();
      tuple = reinterpret_cast<Tuple*>(
          reinterpret_cast<uint8_t*>(tuple) + tuple_desc_->byte_size());
    } else {
      // Reset the null bits, everyhing else will be overwritten
      Tuple::ClearNullBits(tuple, tuple_desc_->null_bytes_offset(),
          tuple_desc_->num_null_bytes());
    }
  }
  return Status::OK();
}

void IcebergMetadataScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env != nullptr) {
    // Close global references
    // if (jmetadata_scanner_ != nullptr) env->DeleteGlobalRef(jmetadata_scanner_);
    // for (auto accessor : jaccessors_) {
    //   if (accessor.second != nullptr) env->DeleteGlobalRef(accessor.second);
    // }
  } else {
    LOG(ERROR) << "Couldn't get JNIEnv, unable to release Global JNI references";
  }
  ScanNode::Close(state);
}

Status IcebergMetadataScanNode::GetCatalogTable(jobject* jtable) {
  Frontend* fe = ExecEnv::GetInstance()->frontend();
  RETURN_IF_ERROR(fe->GetCatalogTable(table_name_, jtable));
  return Status::OK();
}
