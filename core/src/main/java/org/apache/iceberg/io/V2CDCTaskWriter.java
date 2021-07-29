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
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.Tasks;

// TODO: introduce a common parent class for delta task writers?
public abstract class V2CDCTaskWriter<T> implements V2DeltaTaskWriter<T> {
  private final V2PartitionAwareWriter<T, V2DataWriterResult> dataWriter;
  private final V2PartitionAwareWriter<T, V2DeleteWriterResult> equalityDeleteWriter;
  private final V2PartitionAwareWriter<PositionDelete<T>, V2DeleteWriterResult> positionDeleteWriter;
  private final StructProjection keyProjection;
  private final Map<StructLike, PartitionAwarePathOffset> insertedRows;

  private final V2PartitionAwareRow<T> rowWrapper;
  private final PositionDelete<T> positionDelete;
  private final V2PartitionAwareRow<PositionDelete<T>> positionDeleteWrapper;
  private final FileIO io;

  private final List<DataFile> dataFiles = Lists.newArrayList();
  private final List<DeleteFile> deleteFiles = Lists.newArrayList();
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();

  private boolean closed = false;

  public V2CDCTaskWriter(V2PartitionAwareWriter<T, V2DataWriterResult> dataWriter,
                         V2PartitionAwareWriter<T, V2DeleteWriterResult> equalityDeleteWriter,
                         V2PartitionAwareWriter<PositionDelete<T>, V2DeleteWriterResult> positionDeleteWriter,
                         FileIO io, Schema schema, Schema deleteSchema) {
    this.dataWriter = dataWriter;
    this.equalityDeleteWriter = equalityDeleteWriter;
    this.positionDeleteWriter = positionDeleteWriter;
    this.rowWrapper = new V2PartitionAwareRow<>();
    this.positionDelete = new PositionDelete<>();
    this.positionDeleteWrapper = new V2PartitionAwareRow<>();
    this.io = io;
    this.keyProjection = StructProjection.create(schema, deleteSchema);
    this.insertedRows = StructLikeMap.create(deleteSchema.asStruct());
  }

  protected abstract StructLike asStructLike(T data);

  @Override
  public void insert(T row, PartitionSpec spec, StructLike partition) throws IOException {
    CharSequence currentPath = dataWriter.currentPath(spec, partition);
    long currentPosition = dataWriter.currentPosition(spec, partition);
    PartitionAwarePathOffset offset = new PartitionAwarePathOffset(spec, partition, currentPath, currentPosition);

    StructLike copiedKey = StructCopy.copy(keyProjection.wrap(asStructLike(row)));

    PartitionAwarePathOffset previous = insertedRows.put(copiedKey, offset);
    if (previous != null) {
      // TODO: copy original TODO from the old implementation
      positionDelete.set(previous.path(), previous.rowOffset(), null);
      positionDeleteWrapper.wrap(positionDelete, previous.spec(), previous.partition());
      positionDeleteWriter.write(positionDeleteWrapper);
    }

    rowWrapper.wrap(row, spec, partition);
    dataWriter.write(rowWrapper);
  }

  @Override
  public void delete(T row, PartitionSpec spec, StructLike partition) throws IOException {
    StructLike key = keyProjection.wrap(asStructLike(row));
    PartitionAwarePathOffset previous = insertedRows.remove(key);
    if (previous != null) {
      positionDelete.set(previous.path(), previous.rowOffset(), null);
      positionDeleteWrapper.wrap(positionDelete, previous.spec(), previous.partition());
      positionDeleteWriter.write(positionDeleteWrapper);
    }

    rowWrapper.wrap(row, spec, partition);
    equalityDeleteWriter.write(rowWrapper);
  }

  @Override
  public void delete(CharSequence path, long pos, T row, PartitionSpec spec, StructLike partition) throws IOException {
    throw new IllegalArgumentException(this.getClass().getName() + " does not implement explicit position delete");
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
        completeDataWriter(dataWriter);
      }

      if (equalityDeleteWriter != null) {
        completeDeleteWriter(equalityDeleteWriter);
      }

      if (equalityDeleteWriter != null) {
        completeDeleteWriter(equalityDeleteWriter);
      }

      this.closed = true;
    }
  }

  private void completeDataWriter(V2Writer<?, V2DataWriterResult> writer) throws IOException {
    writer.close();

    V2DataWriterResult result = writer.result();
    dataFiles.addAll(result.dataFiles());
  }

  private void completeDeleteWriter(V2Writer<?, V2DeleteWriterResult> writer) throws IOException {
    writer.close();

    V2DeleteWriterResult result = writer.result();
    deleteFiles.addAll(result.deleteFiles());
    referencedDataFiles.addAll(result.referencedDataFiles());
  }

  private static class PartitionAwarePathOffset {
    private final PartitionSpec spec;
    private final StructLike partition;
    private final CharSequence path;
    private final long rowOffset;

    public PartitionAwarePathOffset(PartitionSpec spec, StructLike partition, CharSequence path, long rowOffset) {
      this.spec = spec;
      this.partition = partition;
      this.path = path;
      this.rowOffset = rowOffset;
    }

    public PartitionSpec spec() {
      return spec;
    }

    public StructLike partition() {
      return partition;
    }

    public CharSequence path() {
      return path;
    }

    public long rowOffset() {
      return rowOffset;
    }
  }
}
