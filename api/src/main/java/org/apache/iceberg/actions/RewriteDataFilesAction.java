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

package org.apache.iceberg.actions;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.expressions.Expression;

public interface RewriteDataFilesAction
    extends SnapshotUpdateAction<RewriteDataFilesAction, RewriteDataFilesAction.Result>{

  /**
   * Is it case sensitive
   *
   * @param newCaseSensitive caseSensitive
   * @return this for method chaining
   */
  RewriteDataFilesAction caseSensitive(boolean newCaseSensitive);

  /**
   * Pass a PartitionSpec id to specify which PartitionSpec should be used in DataFile rewrite
   *
   * @param specId PartitionSpec id to rewrite
   * @return this for method chaining
   */
  RewriteDataFilesAction outputSpecId(int specId);

  /**
   * Specify the target rewrite data file size in bytes
   *
   * @param targetSize size in bytes of rewrite data file
   * @return this for method chaining
   */
  RewriteDataFilesAction targetSizeInBytes(long targetSize);

  /**
   * Specify the number of "bins" considered when trying to pack the next file split into a task. Increasing this
   * usually makes tasks a bit more even by considering more ways to pack file regions into a single task with extra
   * planning cost.
   * <p>
   * This configuration can reorder the incoming file regions, to preserve order for lower/upper bounds in file
   * metadata, user can use a lookback of 1.
   *
   * @param lookback number of "bins" considered when trying to pack the next file split into a task.
   * @return this for method chaining
   */
  RewriteDataFilesAction splitLookback(int lookback);

  /**
   * Specify the minimum file size to count to pack into one "bin". If the read file size is smaller than this specified
   * threshold, Iceberg will use this value to do count.
   * <p>
   * this configuration controls the number of files to compact for each task, small value would lead to a high
   * compaction, the default value is 4MB.
   *
   * @param openFileCost minimum file size to count to pack into one "bin".
   * @return this for method chaining
   */
  RewriteDataFilesAction splitOpenFileCost(long openFileCost);

  /**
   * Pass a row Expression to filter DataFiles to be rewritten. Note that all files that may contain data matching the
   * filter may be rewritten.
   *
   * @param expr Expression to filter out DataFiles
   * @return this for method chaining
   */
  RewriteDataFilesAction filter(Expression expr);

  interface Result {
    Iterable<DataFile> deletedDataFiles();
    Iterable<DataFile> addedDataFiles();
  }
}
