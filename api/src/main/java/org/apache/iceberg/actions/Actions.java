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

/**
 * An API for actions performed on a table.
 */
public interface Actions {

  default RemoveOrphanFilesAction removeOrphanFiles() {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement removeOrphanFiles");
  }

  default RewriteManifestsAction rewriteManifests() {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement rewriteManifests");
  }

  default RewriteDataFilesAction rewriteDataFiles() {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement rewriteDataFiles");
  }

  default ExpireSnapshotsAction expireSnapshots() {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not implement expireSnapshots");
  }
}
