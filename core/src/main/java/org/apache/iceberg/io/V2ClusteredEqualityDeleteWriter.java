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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

public class V2ClusteredEqualityDeleteWriter<T> extends V2ClusteredDeleteWriter<T> {

  private final WriterFactory<T> writerFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final FileFormat fileFormat;
  private final long targetFileSizeInBytes;

  public V2ClusteredEqualityDeleteWriter(WriterFactory<T> writerFactory, OutputFileFactory fileFactory,
                                         FileIO io, FileFormat fileFormat, long targetFileSizeInBytes) {
    this.writerFactory = writerFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.fileFormat = fileFormat;
    this.targetFileSizeInBytes = targetFileSizeInBytes;
  }

  @Override
  protected V2RollingEqualityDeleteWriter<T> newWriter(PartitionSpec spec, StructLike partition) {
    return new V2RollingEqualityDeleteWriter<>(
        writerFactory, fileFactory, io, fileFormat, targetFileSizeInBytes, spec, partition);
  }
}
