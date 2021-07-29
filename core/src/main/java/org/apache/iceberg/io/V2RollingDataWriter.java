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

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class V2RollingDataWriter<T> extends V2BaseRollingWriter<T, DataWriter<T>, V2DataWriterResult>{

  private final WriterFactory<T> writerFactory;
  private final List<DataFile> dataFiles;

  public V2RollingDataWriter(WriterFactory<T> writerFactory, OutputFileFactory fileFactory,
                             FileIO io, FileFormat fileFormat, long targetFileSizeInBytes,
                             PartitionSpec spec, StructLike partition) {
    super(fileFactory, io, fileFormat, targetFileSizeInBytes, spec, partition);
    this.writerFactory = writerFactory;
    this.dataFiles = Lists.newArrayList();
    openCurrent();
  }

  @Override
  protected DataWriter<T> newWriter(EncryptedOutputFile file) {
    return writerFactory.newDataWriter(file, spec(), partition());
  }

  @Override
  protected long length(DataWriter<T> writer) {
    return writer.length();
  }

  @Override
  protected void add(V2DataWriterResult result) {
    dataFiles.addAll(result.dataFiles());
  }

  @Override
  protected V2DataWriterResult aggregatedResult() {
    return new V2DataWriterResult(dataFiles);
  }
}
