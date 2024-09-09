/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionStatsUtil {

  private PartitionStatsUtil() {}

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatsUtil.class);

  /**
   * Computes the partition stats for the given snapshot of the table.
   *
   * @param table the table for which partition stats to be computed.
   * @param snapshot the snapshot for which partition stats is computed.
   * @return iterable {@link PartitionStats}
   */
  public static Iterable<PartitionStats> computeStats(Table table, Snapshot snapshot) {
    Preconditions.checkArgument(table != null, "table cannot be null");
    Preconditions.checkArgument(snapshot != null, "snapshot cannot be null");

    StructType partitionType = Partitioning.partitionType(table);
    StructLikeMap<PartitionStats> statsMap = StructLikeMap.createConcurrent(partitionType);
    List<ManifestFile> manifestFiles = snapshot.allManifests(table.io());

    Tasks.foreach(manifestFiles)
        .stopOnFailure()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure((file, e) -> LOG.warn("Failed to process manifest: {}", file.path(), e))
        .run(manifest -> updateStats(table, manifest, partitionType, statsMap));

    return statsMap.values();
  }

  // TODO: this is super ugly and must be refactored into a few separate methods
  private static void updateStats(
      Table table,
      ManifestFile manifest,
      StructType partitionType,
      Map<StructLike, PartitionStats> statsMap) {
    try (ManifestReader<?> reader = openManifest(table, manifest)) {
      for (ManifestEntry<?> entry : reader.entries()) {
        Snapshot snapshot = table.snapshot(entry.snapshotId());
        ContentFile<?> file = entry.file();
        int specId = file.specId();
        PartitionSpec spec = table.specs().get(specId);
        StructLike partition = file.partition();
        StructLike coercedPartition = PartitionUtil.coercePartition(partitionType, spec, partition);
        PartitionStats stats =
            statsMap.computeIfAbsent(
                coercedPartition, ignored -> new PartitionStats(coercedPartition, specId));
        if (entry.isLive()) {
          stats.liveEntry(file, snapshot);
        } else {
          stats.deletedEntry(snapshot);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static ManifestReader<?> openManifest(Table table, ManifestFile manifest) {
    List<String> projection = BaseScan.scanColumns(manifest.content());
    return ManifestFiles.open(manifest, table.io()).select(projection);
  }

  /**
   * Sorts the {@link PartitionStats} based on the partition data.
   *
   * @param stats iterable {@link PartitionStats} which needs to be sorted.
   * @param partitionType unified partition schema.
   * @return Iterator of {@link PartitionStats}
   */
  public static Iterator<PartitionStats> sortStats(
      Iterable<PartitionStats> stats, StructType partitionType) {
    List<PartitionStats> entries = Lists.newArrayList(stats.iterator());
    entries.sort(
        Comparator.comparing(PartitionStats::partition, Comparators.forType(partitionType)));
    return entries.iterator();
  }
}
