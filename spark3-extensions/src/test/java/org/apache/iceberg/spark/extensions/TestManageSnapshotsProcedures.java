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

package org.apache.iceberg.spark.extensions;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import static org.apache.iceberg.TableProperties.WRITE_AUDIT_PUBLISH_ENABLED;

public class TestManageSnapshotsProcedures extends SparkExtensionsTestBase {

  public TestManageSnapshotsProcedures(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void before() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }
  @After
  public void after() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testRollbackToSnapshotUsingPositionalArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    List<Object[]> output = sql(
        "CALL %s.system.rollback_to_snapshot('%s', '%s', %dL)",
        catalogName, tableIdent.namespace(), tableIdent.name(), firstSnapshot.snapshotId());

    assertEquals("Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals("Rollback must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testRollbackToSnapshotUsingNamedArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    List<Object[]> output = sql(
        "CALL %s.system.rollback_to_snapshot(snapshot_id => %dL, namespace => '%s', table => '%s')",
        catalogName, firstSnapshot.snapshotId(), tableIdent.namespace(), tableIdent.name());

    assertEquals("Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals("Rollback must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testRollbackToSnapshotRefreshesRelationCache() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    Dataset<Row> query = spark.sql("SELECT * FROM " + tableName + " WHERE id = 1");
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals("View should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM tmp"));

    List<Object[]> output = sql(
        "CALL %s.system.rollback_to_snapshot(namespace => '%s', table => '%s', snapshot_id => %dL)",
        catalogName, tableIdent.namespace(), tableIdent.name(), firstSnapshot.snapshotId());

    assertEquals("Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals("View cache must be invalidated",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM tmp"));

    sql("UNCACHE TABLE tmp");
  }

  @Test
  public void testRollbackToInvalidSnapshot() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    Namespace namespace = tableIdent.namespace();
    String tableName = tableIdent.name();

    AssertHelpers.assertThrows("Should reject invalid snapshot id",
        ValidationException.class, "Cannot roll back to unknown snapshot id",
        () -> sql("CALL %s.system.rollback_to_snapshot('%s', '%s', -1L)", catalogName, namespace, tableName));
  }

  @Test
  public void testInvalidRollbackToSnapshotCases() {
    AssertHelpers.assertThrows("Should not allow mixed args",
        AnalysisException.class, "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.rollback_to_snapshot(namespace => 'n1', table => 't', 1L)", catalogName));

    AssertHelpers.assertThrows("Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class, "not found",
        () -> sql("CALL %s.custom.rollback_to_snapshot('n', 't', 1L)", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.rollback_to_snapshot('n', 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls with invalid arg types",
        AnalysisException.class, "Wrong arg type for snapshot_id: expected LongType",
        () -> sql("CALL %s.system.rollback_to_snapshot('n', 't', 2.2)", catalogName));

    AssertHelpers.assertThrows("Should reject empty namespace",
        IllegalArgumentException.class, "Namespace cannot be empty",
        () -> sql("CALL %s.system.rollback_to_snapshot('', 't', 1L)", catalogName));

    AssertHelpers.assertThrows("Should reject empty table name",
        IllegalArgumentException.class, "Table name cannot be empty",
        () -> sql("CALL %s.system.rollback_to_snapshot('n', '', 1L)", catalogName));
  }

  @Test
  public void testCherrypickSnapshot() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    spark.conf().set("spark.wap.id", "1");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot wapSnapshot = Iterables.getOnlyElement(table.snapshots());

    List<Object[]> output = sql(
        "CALL %s.system.cherrypick_snapshot('%s', '%s', %dL)",
        catalogName, tableIdent.namespace(), tableIdent.name(), wapSnapshot.snapshotId());

    table.refresh();

    Snapshot currentSnapshot = table.currentSnapshot();

    assertEquals("Procedure output must match",
        ImmutableList.of(row(wapSnapshot.snapshotId(), currentSnapshot.snapshotId())),
        output);

    assertEquals("Cherrypick must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testCherrypickSnapshotRefreshesRelationCache() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    Dataset<Row> query = spark.sql("SELECT * FROM " + tableName + " WHERE id = 1");
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals("View should not produce rows", ImmutableList.of(), sql("SELECT * FROM tmp"));

    spark.conf().set("spark.wap.id", "1");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot wapSnapshot = Iterables.getOnlyElement(table.snapshots());

    sql("CALL %s.system.cherrypick_snapshot('%s', '%s', %dL)",
        catalogName, tableIdent.namespace(), tableIdent.name(), wapSnapshot.snapshotId());

    assertEquals("Cherrypick snapshot should be visible",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM tmp"));

    sql("UNCACHE TABLE tmp");
  }

  @Test
  public void testCherrypickInvalidSnapshot() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    Namespace namespace = tableIdent.namespace();
    String tableName = tableIdent.name();

    AssertHelpers.assertThrows("Should reject invalid snapshot id",
        ValidationException.class, "Cannot cherry pick unknown snapshot id",
        () -> sql("CALL %s.system.cherrypick_snapshot('%s', '%s', -1L)", catalogName, namespace, tableName));
  }

  @Test
  public void testInvalidCherrypickSnapshotCases() {
    AssertHelpers.assertThrows("Should not allow mixed args",
        AnalysisException.class, "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.cherrypick_snapshot('n', table => 't', 1L)", catalogName));

    AssertHelpers.assertThrows("Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class, "not found",
        () -> sql("CALL %s.custom.cherrypick_snapshot('n', 't', 1L)", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.cherrypick_snapshot('n', 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls with invalid arg types",
        AnalysisException.class, "Wrong arg type for snapshot_id: expected LongType",
        () -> sql("CALL %s.system.cherrypick_snapshot('n', 't', 2.2)", catalogName));

    AssertHelpers.assertThrows("Should reject empty namespace",
        IllegalArgumentException.class, "Namespace cannot be empty",
        () -> sql("CALL %s.system.cherrypick_snapshot('', 't', 1L)", catalogName));

    AssertHelpers.assertThrows("Should reject empty table name",
        IllegalArgumentException.class, "Table name cannot be empty",
        () -> sql("CALL %s.system.cherrypick_snapshot('n', '', 1L)", catalogName));
  }

  @Test
  public void testSetCurrentSnapshot() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    spark.conf().set("spark.wap.id", "1");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot wapSnapshot = Iterables.getOnlyElement(table.snapshots());

    List<Object[]> output = sql(
        "CALL %s.system.set_current_snapshot(namespace => '%s', table => '%s', snapshot_id => %dL)",
        catalogName, tableIdent.namespace(), tableIdent.name(), wapSnapshot.snapshotId());

    assertEquals("Procedure output must match",
        ImmutableList.of(row(null, wapSnapshot.snapshotId())),
        output);

    assertEquals("Current snapshot must be set correctly",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));
  }
  @Test
  public void testExpireSnapshotByRetainNum() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    // create first snapshot
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();
    int numSnapshot = 0;
    for (Snapshot snapshot : table.snapshots()) {
      numSnapshot++;
    }
    Assert.assertEquals(1, numSnapshot);
    // create second snapshot
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (3, 'c')", tableName);
    table.refresh();
    numSnapshot = 0;
    Snapshot secondSnapshot = table.currentSnapshot();
    for (Snapshot snapshot : table.snapshots()) {
      numSnapshot++;
    }
    Assert.assertEquals(3, numSnapshot);

    // expire snapshot by stored procedure
    // num of snapshot is 3
    // retain num is 2
    List<Object[]> output = sql(
        "CALL %s.system.expire_snapshot(namespace => '%s', table => '%s', retain_last => %d)",
        catalogName, tableIdent.namespace(), tableIdent.name(), 2);
    assertEquals("return the retain num for expire snapshot",
        ImmutableList.of(row(2)),
        output);

    table.refresh();
    numSnapshot = 0;
    for (Snapshot snapshot : table.snapshots()) {
      numSnapshot++;
    }
    Assert.assertEquals(2, numSnapshot);

    // retain num is 1
    output = sql(
        "CALL %s.system.expire_snapshot(namespace => '%s', table => '%s', retain_last => %d)",
        catalogName, tableIdent.namespace(), tableIdent.name(), 1);
    assertEquals("return the retain num for expire snapshot",
        ImmutableList.of(row(1)),
        output);
    table.refresh();
    numSnapshot = 0;
    for (Snapshot snapshot : table.snapshots()) {
      numSnapshot++;
    }
    Assert.assertEquals(1, numSnapshot);
  }

  @Test
  public void testExpireSnapshotWithInvalidArgs() {
    // invalid num for retain snapshot
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    // create first snapshot
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    // invalid num for retain snapshot
    int numRetainSnapshot = -1;
    AssertHelpers.assertThrows("invalid retain num for snapshot",
        IllegalArgumentException.class, String.format("Number of snapshots to retain must be at least 1, cannot be: %s", numRetainSnapshot),
        () -> sql("CALL %s.system.expire_snapshot(namespace => '%s', table => '%s', retain_last => %d)",
            catalogName, tableIdent.namespace(), tableIdent.name(), numRetainSnapshot));
  }
  // TODO: should we allow quoted namespaces?
}
