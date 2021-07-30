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

package org.apache.iceberg.io;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Tasks;

public abstract class BaseDeltaTaskWriter<T> implements DeltaTaskWriter<T> {

  private final List<DataFile> dataFiles = Lists.newArrayList();
  private final List<DeleteFile> deleteFiles = Lists.newArrayList();
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
  private final FileIO io;

  private boolean closed = false;

  public BaseDeltaTaskWriter(FileIO io) {
    this.io = io;
  }

  protected FileIO io() {
    return io;
  }

  @Override
  public void abort() throws IOException {
    Preconditions.checkState(closed, "Cannot abort unclosed task writer");

    Tasks.foreach(Iterables.concat(dataFiles, deleteFiles))
        .suppressFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  @Override
  public Result result() {
    Preconditions.checkState(closed, "Cannot obtain result from unclosed task writer");
    return new BaseDeltaTaskWriteResult(dataFiles, deleteFiles, referencedDataFiles);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closeWriters();
      this.closed = true;
    }
  }

  protected abstract void closeWriters() throws IOException;

  protected void closeDataWriter(PartitionAwareWriter<?, DataWriteResult> writer) throws IOException {
    writer.close();

    DataWriteResult result = writer.result();
    dataFiles.addAll(result.dataFiles());
  }

  protected void closeDeleteWriter(PartitionAwareWriter<?, DeleteWriteResult> deleteWriter) throws IOException {
    deleteWriter.close();

    DeleteWriteResult result = deleteWriter.result();
    deleteFiles.addAll(result.deleteFiles());
    referencedDataFiles.addAll(result.referencedDataFiles());
  }
}
