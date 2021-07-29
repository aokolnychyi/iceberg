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
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.V2Writer.Result;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructLikeMap;

public abstract class V2FanoutWriter<T, R extends Result> implements V2PartitionAwareWriter<T, R> {

  private final Map<Integer, Map<StructLike, V2Writer<T, R>>> writers = Maps.newHashMap();
  private boolean closed = false;

  protected abstract V2Writer<T, R> newWriter(PartitionSpec spec, StructLike partition);

  protected abstract void add(R result);

  protected abstract R aggregatedResult();

  @Override
  public void write(V2PartitionAwareRow<T> payload) throws IOException {
    doWrite(payload.row(), payload.spec(), payload.partition());
  }

  private void doWrite(T row, PartitionSpec spec, StructLike partition) throws IOException {
    Map<StructLike, V2Writer<T, R>> specWriters = writers.computeIfAbsent(spec.specId(), id -> StructLikeMap.create(spec.partitionType()));
    V2Writer<T, R> writer = specWriters.get(partition);

    if (writer == null) {
      // copy the partition key as the key object is reused
      StructLike copiedPartition = StructCopy.copy(partition);
      writer = newWriter(spec, copiedPartition);
      specWriters.put(copiedPartition, writer);
    }

    writer.write(row);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closeWriters();
      this.closed = true;
    }
  }

  private void closeWriters() throws IOException {
    for (Map<StructLike, V2Writer<T, R>> specWriters : writers.values()) {
      for (V2Writer<T, R> writer : specWriters.values()) {
        writer.close();

        R result = writer.result();
        add(result);
      }

      specWriters.clear();
    }

    writers.clear();
  }

  @Override
  public final R result() {
    Preconditions.checkState(closed, "Cannot get result from unclosed writer");
    return aggregatedResult();
  }
}
