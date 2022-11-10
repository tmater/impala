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

package org.apache.impala.catalog.iceberg;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.EnumUtils;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.VirtualTable;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.IcebergSchemaConverter;

/**
 * Iceberg metadtata tables.
 */
public class IcebergMetadataTable extends VirtualTable {
  private FeIcebergTable baseTable_;

  public IcebergMetadataTable(FeTable baseTable, List<String> tblRefPath)
      throws ImpalaRuntimeException {
    super(null, baseTable.getDb(), baseTable.getName(), baseTable.getOwnerUser());
    baseTable_ = (FeIcebergTable) baseTable;
    String metadataTableTypeString = tblRefPath.get(2);
    MetadataTableType type = MetadataTableType.valueOf(
        metadataTableTypeString.toUpperCase());
    Table metadataTable = MetadataTableUtils.createMetadataTableInstance(
        baseTable_.getIcebergApiTable(), type);
    Schema metadataTableSchema = metadataTable.schema();
    try {
      for (Column col : IcebergSchemaConverter.convertToImpalaSchema(metadataTableSchema)) {
        addColumn(col);
      }
    } catch (Exception e) {
      throw
    }
  }

  @Override
  public long getNumRows() {
    return -1;
  }

  public FeIcebergTable getBaseTable() {
    return baseTable_;
  }

  @Override
  public TTableStats getTTableStats() {
    long totalBytes = 0;
    TTableStats ret = new TTableStats(getNumRows());
    ret.setTotal_file_bytes(totalBytes);
    return ret;
  }

  /**
   * Return same descriptor as the base table, but with a schema that corresponds to
   * the metadtata table schema.
   */
  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor desc = baseTable_.toThriftDescriptor(tableId, referencedPartitions);
    desc.setColumnDescriptors(FeCatalogUtils.getTColumnDescriptors(this));
    return desc;
  }

  /**
   * Returns true if the table ref is referring to a valid metadata table.
   */
  public static Boolean isIcebergMetadataTable(List<String> tblRefPath) {
    if (tblRefPath == null) return false;
    if (tblRefPath.size() != 3) return false;
    String vTableName = tblRefPath.get(2).toUpperCase();
    return EnumUtils.isValidEnum(MetadataTableType.class, vTableName);
  }
}
