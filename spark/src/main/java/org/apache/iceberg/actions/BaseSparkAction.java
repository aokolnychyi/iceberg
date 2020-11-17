/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.actions;

import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.ClosingIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.iceberg.MetadataTableType.ALL_MANIFESTS;

abstract class BaseSparkAction<R> extends BaseAction<R> {

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final boolean isSpark2;
  private final Table table;
  private final TableOperations ops;

  public BaseSparkAction(SparkSession spark, Table table) {
    this.spark = spark;
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
    this.isSpark2 = sparkContext.version().startsWith("2");
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();
  }

  protected SparkSession spark() {
    return spark;
  }

  protected JavaSparkContext sparkContext() {
    return sparkContext;
  }

  @Override
  protected Table table() {
    return table;
  }

  protected TableOperations ops() {
    return ops;
  }

  protected Dataset<Row> buildValidDataFileDF() {
    return buildValidDataFileDF(table.name());
  }

  protected Dataset<Row> buildValidDataFileDF(String tableName) {
    Broadcast<FileIO> io = sparkContext.broadcast(SparkUtil.serializableFileIO(table()));

    Dataset<ManifestFileBean> allManifests = loadMetadataTable(tableName, ALL_MANIFESTS)
        .selectExpr("path", "length", "partition_spec_id as partitionSpecId", "added_snapshot_id as addedSnapshotId")
        .dropDuplicates("path")
        .repartition(spark.sessionState().conf().numShufflePartitions()) // avoid adaptive execution combining tasks
        .as(Encoders.bean(ManifestFileBean.class));

    return allManifests.flatMap(new ReadManifest(io), Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildManifestFileDF() {
    return buildManifestFileDF(table.name());
  }

  protected Dataset<Row> buildManifestFileDF(String tableName) {
    return loadMetadataTable(tableName, ALL_MANIFESTS).selectExpr("path as file_path");
  }

  protected Dataset<Row> buildManifestListDF() {
    List<String> manifestLists = getManifestListPaths(table.snapshots());
    return spark.createDataset(manifestLists, Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildManifestListDF(String metadataFileLocation) {
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, table.io());
    BaseTable staticTable = new BaseTable(ops, table.name());
    List<String> manifestLists = getManifestListPaths(staticTable.snapshots());
    return spark.createDataset(manifestLists, Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildOtherMetadataFileDF() {
    List<String> otherMetadataFiles = getOtherMetadataFilePaths(ops);
    return spark.createDataset(otherMetadataFiles, Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildValidMetadataFileDF() {
    Dataset<Row> manifestDF = buildManifestFileDF();
    Dataset<Row> manifestListDF = buildManifestListDF();
    Dataset<Row> otherMetadataFileDF = buildOtherMetadataFileDF();

    return manifestDF.union(otherMetadataFileDF).union(manifestListDF);
  }

  // TODO: spark_catalog will fail?
  // TODO: try BaseSpark2Action and BaseSpark3Action instead?
  protected Dataset<Row> loadMetadataTable(String tableName, MetadataTableType type) {
    if (tableName.contains("/")) {
      // use the DataFrame API for path-based tables in Spark 2 and 3
      String metadataTableName = tableName + "#" + type;
      return spark.read().format("iceberg").load(metadataTableName);
    } else if (isSpark2 && tableName.startsWith("hadoop.")) {
      // for HadoopCatalog tables in Spark 2, use the table location to load the metadata table
      // because IcebergCatalog uses HiveCatalog when the table is identified by name
      String metadataTableName = table.location() + "#" + type;
      return spark.read().format("iceberg").load(metadataTableName);
    } else if (isSpark2 && tableName.startsWith("hive.")) {
      // HiveCatalog prepends a logical name which we need to drop for Spark 2.4
      String metadataTableName = tableName.replaceFirst("hive\\.", "") + "." + type;
      return spark.read().format("iceberg").load(metadataTableName);
    } else {
      // use catalog-based resolution in all other cases
      String metadataTableName = tableName + "." + type;
      return spark.table(metadataTableName);
    }
  }

  private static class ReadManifest implements FlatMapFunction<ManifestFileBean, String> {
    private final Broadcast<FileIO> io;

    ReadManifest(Broadcast<FileIO> io) {
      this.io = io;
    }

    @Override
    public Iterator<String> call(ManifestFileBean manifest) {
      return new ClosingIterator<>(ManifestFiles.readPaths(manifest, io.getValue()).iterator());
    }
  }
}
