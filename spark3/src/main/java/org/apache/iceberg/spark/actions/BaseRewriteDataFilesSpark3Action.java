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

package org.apache.iceberg.spark.actions;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.FileScanTaskSetManager;
import org.apache.iceberg.spark.actions.rewrite.Spark3BinPackStrategy;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseRewriteDataFilesSpark3Action extends BaseRewriteDataFilesSparkAction {
  private static final Logger LOG = LoggerFactory.getLogger(BaseRewriteDataFilesSpark3Action.class);
  private final FileRewriteCoordinator coordinator = FileRewriteCoordinator.get();

  protected BaseRewriteDataFilesSpark3Action(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  protected RewriteDataFiles self() {
    return this;
  }

  @Override
  protected DataFilesRewriter newRewriter(RewriteStrategy strategy) {
    // TODO: return a correct rewriter
    return null;
  }

  static class Spark3DataFilesBinPacker implements DataFilesRewriter {
    private final Table table;
    private final SparkSession spark;
    private final FileScanTaskSetManager taskSetManager = FileScanTaskSetManager.get();
    private final FileRewriteCoordinator rewriteCoordinator = FileRewriteCoordinator.get();

    public Spark3DataFilesBinPacker(Table table, SparkSession spark) {
      this.table = table;
      this.spark = spark;
    }

    @Override
    public Set<DataFile> rewrite(String groupID, List<FileScanTask> fileScanTasks) {
      // TODO: copy from Spark3BinPackStrategy
      return null;
    }

    @Override
    public void commit(Set<String> groupIDs) {
      rewriteCoordinator.commitRewrite(table, groupIDs);
    }

    @Override
    public void abort(String groupID) {
      try {
        rewriteCoordinator.abortRewrite(table, groupID);
      } catch (Exception e) {
        LOG.error("Unable to cleanup rewrite file group {} for table {}", groupID, table.name(), e);
      }
    }
  }
}
