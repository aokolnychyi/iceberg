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
import java.util.function.Supplier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Tasks;

public class V2BaseDeltaTaskWriter<T> implements V2DeltaTaskWriter<T> {

  private final V2PartitionAwareWriter<T, V2DataWriterResult> dataWriter;
  private final V2PartitionAwareWriter<T, V2DeleteWriterResult> equalityDeleteWriter;
  private final V2PartitionAwareWriter<PositionDelete<T>, V2DeleteWriterResult> positionDeleteWriter;

  private final V2PartitionAwareRow<T> rowWrapper;
  private final PositionDelete<T> positionDelete;
  private final V2PartitionAwareRow<PositionDelete<T>> positionDeleteWrapper;
  private final FileIO io;

  private final List<DataFile> dataFiles = Lists.newArrayList();
  private final List<DeleteFile> deleteFiles = Lists.newArrayList();
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();

  private boolean closed = false;

  public V2BaseDeltaTaskWriter(V2PartitionAwareWriter<T, V2DataWriterResult> dataWriter,
                               V2PartitionAwareWriter<PositionDelete<T>, V2DeleteWriterResult> positionDeleteWriter,
                               FileIO io) {
    this(dataWriter, null, positionDeleteWriter, io);
  }

  public V2BaseDeltaTaskWriter(V2PartitionAwareWriter<T, V2DataWriterResult> dataWriter,
                               V2PartitionAwareWriter<T, V2DeleteWriterResult> equalityDeleteWriter,
                               V2PartitionAwareWriter<PositionDelete<T>, V2DeleteWriterResult> positionDeleteWriter,
                               FileIO io) {
    this.dataWriter = dataWriter;
    this.equalityDeleteWriter = equalityDeleteWriter;
    this.positionDeleteWriter = positionDeleteWriter;
    this.rowWrapper = new V2PartitionAwareRow<>();
    this.positionDelete = new PositionDelete<>();
    this.positionDeleteWrapper = new V2PartitionAwareRow<>();
    this.io = io;
  }

  protected FileIO io() {
    return io;
  }

  @Override
  public void insert(T row, PartitionSpec spec, StructLike partition) throws IOException {
    rowWrapper.wrap(row, spec, partition);
    dataWriter.write(rowWrapper);
  }

  @Override
  public void delete(T row, PartitionSpec spec, StructLike partition) throws IOException {
    rowWrapper.wrap(row, spec, partition);
    equalityDeleteWriter.write(rowWrapper);
  }

  @Override
  public void delete(CharSequence path, long pos, T row, PartitionSpec spec, StructLike partition) throws IOException {
    positionDelete.set(path, pos, row);
    positionDeleteWrapper.wrap(positionDelete, spec, partition);
    positionDeleteWriter.write(positionDeleteWrapper);
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
    return new V2BaseDeltaTaskWriteResult(dataFiles, deleteFiles, referencedDataFiles);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {

      if (dataWriter != null) {
        closeDataWriter(dataWriter);
      }

      if (equalityDeleteWriter != null) {
        closeDeleteWriter(equalityDeleteWriter);
      }

      if (positionDeleteWriter != null) {
        closeDeleteWriter(positionDeleteWriter);
      }

      this.closed = true;
    }
  }

  private void closeDataWriter(V2Writer<?, V2DataWriterResult> dataWriter) throws IOException {
    dataWriter.close();

    V2DataWriterResult result = dataWriter.result();
    dataFiles.addAll(result.dataFiles());
  }

  private void closeDeleteWriter(V2Writer<?, V2DeleteWriterResult> deleteWriter) throws IOException {
    deleteWriter.close();

    V2DeleteWriterResult result = deleteWriter.result();
    deleteFiles.addAll(result.deleteFiles());
    referencedDataFiles.addAll(result.referencedDataFiles());
  }
}
