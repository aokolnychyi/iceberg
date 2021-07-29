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
import java.util.Comparator;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.V2Writer.Result;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;

// a writer capable of writing to multiple specs and partitions ensuring the incoming data is properly clustered
public abstract class V2ClusteredWriter<T, R extends Result> implements V2PartitionAwareWriter<T, R> {

  private final Set<Integer> completedSpecs = Sets.newHashSet();

  private PartitionSpec currentSpec = null;
  private Comparator<StructLike> partitionComparator = null;
  private Set<StructLike> completedPartitions = null;
  private StructLike currentPartition = null;
  private V2Writer<T, R> currentWriter = null;

  private boolean closed = false;

  protected abstract V2Writer<T, R> newWriter(PartitionSpec spec, StructLike partition);

  protected abstract void add(R result);

  protected abstract R aggregatedResult();

  public void write(V2PartitionAwareRow<T> payload) throws IOException {
    doWrite(payload.row(), payload.spec(), payload.partition());
  }

  private void doWrite(T row, PartitionSpec spec, StructLike partition) throws IOException {
    if (!spec.equals(currentSpec)) {
      if (currentSpec != null) {
        closeCurrent();
        completedSpecs.add(currentSpec.specId());
        completedPartitions.clear();
      }

      if (completedSpecs.contains(spec.specId())) {
        throw new IllegalStateException("Already closed files for spec: " + spec.specId());
      }

      Types.StructType partitionType = spec.partitionType();

      currentSpec = spec;
      partitionComparator = Comparators.forType(partitionType);
      completedPartitions = StructLikeSet.create(partitionType);
      // copy the partition key as the key object is reused
      currentPartition = partition != null ? StructCopy.copy(partition) : null;
      currentWriter = newWriter(currentSpec, currentPartition);

    } else if (partition != currentPartition && partitionComparator.compare(partition, currentPartition) != 0) {
      closeCurrent();
      completedPartitions.add(currentPartition);

      if (completedPartitions.contains(partition)) {
        String path = spec.partitionToPath(partition);
        throw new IllegalStateException("Already closed files for partition: " + path);
      }

      // copy the partition key as the key object is reused
      currentPartition = partition != null ? StructCopy.copy(partition) : null;
      currentWriter = newWriter(currentSpec, currentPartition);
    }

    currentWriter.write(row);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closeCurrent();
      this.closed = true;
    }
  }

  private void closeCurrent() throws IOException {
    if (currentWriter != null) {
      currentWriter.close();

      R result = currentWriter.result();
      add(result);

      this.currentWriter = null;
    }
  }

  @Override
  public final R result() {
    Preconditions.checkState(closed, "Cannot get result from unclosed writer");
    return aggregatedResult();
  }
}
