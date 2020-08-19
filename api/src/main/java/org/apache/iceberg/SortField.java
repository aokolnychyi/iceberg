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

import java.io.Serializable;
import java.util.Objects;
import org.apache.iceberg.transforms.Transform;

public class SortField implements Serializable {
  // TODO: make top-level?
  public enum Direction {
    ASC, DESC
  }

  public enum NullOrder {
    NULLS_FIRST, NULLS_LAST
  }

  // TODO: do we need name?
  private final int sourceId;
  private final Transform<?, ?> transform;
  private final Direction direction;
  private final NullOrder nullOrder;

  SortField(int sourceId, Transform<?, ?> transform, Direction direction, NullOrder nullOrder) {
    this.sourceId = sourceId;
    this.transform = transform;
    this.direction = direction;
    this.nullOrder = nullOrder;
  }

  public int sourceId() {
    return sourceId;
  }

  public Transform<?, ?> transform() {
    return transform;
  }

  public Direction direction() {
    return direction;
  }

  public NullOrder nullOrder() {
    return nullOrder;
  }

  public boolean sameOrder(SortField that) {
    return sourceId == that.sourceId &&
        transform.equals(that.transform) &&
        direction == that.direction &&
        nullOrder == that.nullOrder;
  }

  // TODO: toString

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    SortField that = (SortField) other;
    return sourceId == that.sourceId &&
        transform.equals(that.transform) &&
        direction == that.direction &&
        nullOrder == that.nullOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceId, transform, direction, nullOrder);
  }
}
