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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceSet;

public class V2FanoutSortedPositionDeleteWriter<T> extends V2FanoutWriter<PositionDelete<T>, V2DeleteWriterResult> {
  private final FileAppenderFactory<T> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileFormat fileFormat;

  private final List<DeleteFile> deleteFiles = Lists.newArrayList();
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();

  public V2FanoutSortedPositionDeleteWriter(FileAppenderFactory<T> appenderFactory,
                                            OutputFileFactory fileFactory, FileFormat fileFormat) {
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.fileFormat = fileFormat;
  }

  @Override
  protected V2Writer<PositionDelete<T>, V2DeleteWriterResult> newWriter(PartitionSpec spec, StructLike partition) {
    return new SortedPosDeleteWriter<>(appenderFactory, fileFactory, fileFormat, partition);
  }

  @Override
  protected void add(V2DeleteWriterResult result) {
    deleteFiles.addAll(result.deleteFiles());
    referencedDataFiles.addAll(result.referencedDataFiles());
  }

  @Override
  protected V2DeleteWriterResult aggregatedResult() {
    return new V2DeleteWriterResult(deleteFiles, referencedDataFiles);
  }
}
