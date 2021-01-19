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

import java.util.function.Predicate;
import org.apache.iceberg.ManifestFile;

public interface RewriteManifestsAction
    extends SnapshotUpdateAction<RewriteManifestsAction, RewriteManifestsAction.Result> {

  RewriteManifestsAction specId(int specId);

  /**
   * Rewrites only manifests that match the given predicate.
   *
   * @param predicate a predicate
   * @return this for method chaining
   */
  // TODO: can we replace with an expression? Should we?
  RewriteManifestsAction rewriteIf(Predicate<ManifestFile> predicate);

  /**
   * Passes a location where the manifests should be written.
   *
   * @param stagingLocation a staging location
   * @return this for method chaining
   */
  RewriteManifestsAction stagingLocation(String stagingLocation);

  /**
   * Configures whether the action should cache manifest entries used in multiple jobs.
   *
   * @param newUseCaching a flag whether to use caching
   * @return this for method chaining
   */
  // TODO: is it too Spark specific?
  RewriteManifestsAction useCaching(boolean newUseCaching);

  interface Result {
    Iterable<ManifestFile> deletedManifests();
    Iterable<ManifestFile> addedManifests();
  }
}
