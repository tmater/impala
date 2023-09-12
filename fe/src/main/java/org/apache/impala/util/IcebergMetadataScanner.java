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

package org.apache.impala.util;

import org.apache.iceberg.Accessor;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.StructLike;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterator;


/**
 * Metadata scanner class for Iceberg. Instantiated and governed by the backend.
 */
public class IcebergMetadataScanner {
  final static Logger LOG = LoggerFactory.getLogger(IcebergMetadataScanner.class);
  // This object is extracted by the backend and passed when this object is constructed.
  FeIcebergTable iceTbl_ = null;

  // Metadata table
  Table metadataTable_ = null;

  // Name of the metadata table.
  String metadataTableName_;


  // Persist the file scan task iterator so we can continue after a batch is done.
  CloseableIterator<FileScanTask> fileScanTaskIterator_;

  // Persist the data rows iterator, so we can continue after a batch is filled.
  CloseableIterator<StructLike> dataRowsIterator_;

  IcebergMetadataScanner(FeTable iceTbl, String metadataTableName) {
    this.iceTbl_ = (FeIcebergTable) iceTbl;
    this.metadataTableName_ = metadataTableName;
    LOG.info("TMATE: constructor " + iceTbl_.getTableName());
  }

  void ScanMetadataTable() {
    LOG.info("TMATE: Scanner");
    metadataTable_ = MetadataTableUtils.createMetadataTableInstance(
        iceTbl_.getIcebergApiTable(), MetadataTableType.valueOf(metadataTableName_));
    TableScan scan = metadataTable_.newScan();
    LOG.info("TMATE: Scanner");
    // init the FileScanTask iterator and DataRowsIterator
    fileScanTaskIterator_ = scan.planFiles().iterator();
    LOG.info("TMATE: Scanner");
    while (fileScanTaskIterator_.hasNext()) {
      DataTask dataTask = (DataTask)fileScanTaskIterator_.next();
      dataRowsIterator_ = dataTask.rows().iterator();
      if (dataRowsIterator_.hasNext()) {
        break;
      }
    }
  }

  @Override
  protected void finalize() throws Throwable {
        LOG.info("TMATE " + "destructor"); 
    super.finalize();
  }

  Accessor GetAccessor(int slotColPos) {
    int fieldId = metadataTable_.schema().columns().get(slotColPos).fieldId();
    return metadataTable_.schema().accessorForField(fieldId);
  }

  StructLike GetNext() {
    // Return the next row in the DataRows iterator
    if (dataRowsIterator_.hasNext()) {
      return dataRowsIterator_.next();
    }
    // Otherwise this DataTask is empty, find a FileScanTask that is 
    while (fileScanTaskIterator_.hasNext()) {
      DataTask dataTask = (DataTask)fileScanTaskIterator_.next();
      dataRowsIterator_ = dataTask.rows().iterator();
      if (dataRowsIterator_.hasNext()) {
        return dataRowsIterator_.next();
      }
    }
    return null;
  }


}
