/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.actions;

import org.apache.iceberg.Table;
import org.apache.spark.sql.SparkSession;

// TODO: have BaseSparkActions shared between Spark 2 and 3?
public class SparkActions implements Actions {

  private final SparkSession spark;
  private final Table table;

  protected SparkActions(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
  }

  public static SparkActions forTable(SparkSession spark, Table table) {
    return new SparkActions(spark, table);
  }

  public static SparkActions forTable(Table table) {
    return new SparkActions(SparkSession.active(), table);
  }

  @Override
  public RemoveOrphanFilesAction removeOrphanFiles() {
    return new BaseRemoveOrphanFilesSparkAction(spark, table);
  }

  @Override
  public RewriteManifestsAction rewriteManifests() {
    return new BaseRewriteManifestsSparkAction(spark, table);
  }

  @Override
  public ExpireSnapshotsAction expireSnapshots() {
    return new BaseExpireSnapshotsSparkAction(spark, table);
  }
}
