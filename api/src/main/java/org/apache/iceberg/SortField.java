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
import org.apache.iceberg.expressions.BoundTerm;

public class SortField implements Serializable {

  private final BoundTerm<?> term;
  private final SortDirection direction;
  private final NullOrder nullOrder;

  SortField(BoundTerm<?> term, SortDirection direction, NullOrder nullOrder) {
    this.term = term;
    this.direction = direction;
    this.nullOrder = nullOrder;
  }

  @SuppressWarnings("unchecked")
  public <T> BoundTerm<T> term() {
    return (BoundTerm<T>) term;
  }

  public SortDirection direction() {
    return direction;
  }

  public NullOrder nullOrder() {
    return nullOrder;
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
    // TODO: implement equals in terms
    return term.equals(that.term) && direction == that.direction && nullOrder == that.nullOrder;
  }

  @Override
  public int hashCode() {
    // TODO: implement hashCode in terms
    return Objects.hash(term, direction, nullOrder);
  }
}
