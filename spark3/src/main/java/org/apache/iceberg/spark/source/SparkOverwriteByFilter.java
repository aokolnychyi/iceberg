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

package org.apache.iceberg.spark.source;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class SparkOverwriteByFilter extends SparkBatchWrite {

  private final Expression overwriteExpr;

  SparkOverwriteByFilter(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager,
                         CaseInsensitiveStringMap options, String applicationId, String wapId,
                         Schema writeSchema, StructType dsSchema, Expression overwriteExpr) {
    super(table, io, encryptionManager, options, applicationId, wapId, writeSchema, dsSchema);
    this.overwriteExpr = overwriteExpr;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    OverwriteFiles overwriteFiles = table().newOverwrite();
    overwriteFiles.overwriteByRowFilter(overwriteExpr);

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      overwriteFiles.addFile(file);
    }

    String commitMsg = String.format("overwrite by filter %s with %d new data files", overwriteExpr, numFiles);
    commitOperation(overwriteFiles, commitMsg);
  }
}
