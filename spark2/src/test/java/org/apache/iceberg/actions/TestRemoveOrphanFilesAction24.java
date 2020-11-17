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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

public class TestRemoveOrphanFilesAction24 extends TestRemoveOrphanFilesAction {

  @Test
  public void testRemoveOrphanFilesWithHadoopCatalog() throws InterruptedException {
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), tableLocation);
    String namespaceName = "testDb";
    String tableName = "testTb";

    Namespace namespace = Namespace.of(namespaceName);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
    Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap());

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(table.location());

    df.write().mode("append").parquet(table.location() + "/data");

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    table.refresh();

    List<String> result = Actions.forTable(table)
        .removeOrphanFiles()
        .olderThan(System.currentTimeMillis())
        .execute();

    Assert.assertEquals("Should delete only 1 files", 1, result.size());

    Dataset<Row> resultDF = spark.read().format("iceberg").load(table.location());
    List<ThreeColumnRecord> actualRecords = resultDF
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testHiveCatalogTable() throws IOException {
    Table table = catalog.createTable(TableIdentifier.of("default", "hivetestorphan"), SCHEMA, SPEC, tableLocation,
        Maps.newHashMap());

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save("default.hivetestorphan");

    String location = table.location().replaceFirst("file:", "");
    new File(location + "/data/trashfile").createNewFile();

    List<String> results = Actions.forTable(table).removeOrphanFiles()
        .olderThan(System.currentTimeMillis() + 1000).execute();
    Assert.assertTrue("trash file should be removed",
        results.contains("file:" + location + "data/trashfile"));
  }
}
