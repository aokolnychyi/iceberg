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

package org.apache.iceberg.io;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;

public class V2BaseTaskWriter<T> implements V2TaskWriter<T> {
  private final V2PartitionAwareWriter<T, V2DataWriterResult> writer;
  private final V2PartitionAwareRow<T> rowWrapper;
  private final FileIO io;
  private final List<DataFile> dataFiles;

  private boolean closed = false;

  public V2BaseTaskWriter(V2PartitionAwareWriter<T, V2DataWriterResult> writer, FileIO io) {
    this.writer = writer;
    this.io = io;
    this.dataFiles = Lists.newArrayList();
    this.rowWrapper = new V2PartitionAwareRow<>();
  }

  @Override
  public void insert(T row, PartitionSpec spec, StructLike partition) throws IOException {
    rowWrapper.wrap(row, spec, partition);
    writer.write(rowWrapper);
  }

  @Override
  public void abort() throws IOException {
    Preconditions.checkState(closed, "Cannot abort unclosed task writer");

    Tasks.foreach(dataFiles)
        .suppressFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  @Override
  public Result result() {
    Preconditions.checkState(closed, "Cannot obtain result from unclosed task writer");
    return new BaseV2TaskWriteResult(dataFiles);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      if (writer != null) {
        writer.close();

        V2DataWriterResult result = writer.result();
        dataFiles.addAll(result.dataFiles());
      }

      this.closed = true;
    }
  }

  private static class BaseV2TaskWriteResult implements V2TaskWriter.Result {
    private final DataFile[] dataFiles;

    public BaseV2TaskWriteResult(List<DataFile> dataFiles) {
      this.dataFiles = dataFiles.toArray(new DataFile[0]);
    }

    @Override
    public DataFile[] dataFiles() {
      return dataFiles;
    }
  }
}
