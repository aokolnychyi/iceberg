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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.expressions.UnboundTransform;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

public class SortOrder implements Serializable {
  // TODO: shall we reserve 0 for the unsorted order? PartitionSpec does not guarantee it?
  private static final SortOrder UNSORTED_ORDER = new SortOrder(new Schema(), 0, Collections.emptyList());

  // TODO: do we need schema in SortOrder?
  private final Schema schema;
  private final int orderId;
  private final SortField[] fields;

  private transient volatile List<SortField> fieldList;

  private SortOrder(Schema schema, int orderId, List<SortField> fields) {
    this.schema = schema;
    this.orderId = orderId;
    this.fields = fields.toArray(new SortField[0]);
  }

  public Schema schema() {
    return schema;
  }

  public int orderId() {
    return orderId;
  }

  public List<SortField> fields() {
    return lazyFieldList();
  }

  public boolean isUnsorted() {
    return fields.length < 1;
  }

  public boolean satisfies(SortOrder anotherSortOrder) {
    if (anotherSortOrder.isUnsorted()) {
      return true;
    }

    if (anotherSortOrder.fields.length > fields.length) {
      return false;
    }

    return IntStream.range(0, anotherSortOrder.fields.length)
        .allMatch(index -> fields[index].sameOrder(anotherSortOrder.fields[index]));
  }

  private List<SortField> lazyFieldList() {
    if (fieldList == null) {
      synchronized (this) {
        if (fieldList == null) {
          this.fieldList = ImmutableList.copyOf(fields);
        }
      }
    }
    return fieldList;
  }

  // TODO: toString

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    SortOrder that = (SortOrder) other;
    return orderId == that.orderId && Arrays.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return 31 * Integer.hashCode(orderId) + Arrays.hashCode(fields);
  }

  public static Builder builderFor(Schema schema) {
    return new Builder(schema);
  }

  // TODO: add validation
  public static class Builder {
    // TODO: case sensitivity
    private final Schema schema;
    private final List<SortField> fields = Lists.newArrayList();
    private int orderId = 0;
    private boolean caseSensitive = true;

    private Builder(Schema schema) {
      this.schema = schema;
    }

    public Builder orderBy(String name) {
      Types.NestedField column = findColumn(name);
      UnboundTerm<?> term = Expressions.transform(name, Transforms.identity(column.type()));
      return orderBy(term);
    }

    public Builder orderBy(String name, SortField.Direction direction, SortField.NullOrder nullOrder) {
      Types.NestedField column = findColumn(name);
      UnboundTerm<?> term = Expressions.transform(name, Transforms.identity(column.type()));
      return orderBy(term, direction, nullOrder);
    }

    public Builder orderBy(Term term) {
      return orderBy(term, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
    }

    public Builder orderBy(Term term, SortField.Direction direction, SortField.NullOrder nullOrder) {
      return this;
    }

    public Builder withOrderId(int newOrderId) {
      this.orderId = newOrderId;
      return this;
    }

    public Builder caseSensitive(boolean sortCaseSensitive) {
      this.caseSensitive = sortCaseSensitive;
      return this;
    }

    public SortOrder build() {
      return new SortOrder(schema, orderId, fields);
    }

    private Transform<?, ?> toTransform(Term term) {
      // TODO: either build a utility class or expose asTransform on Term
      return null;
    }

    private Types.NestedField findColumn(String name) {
      Types.NestedField column = schema.findField(name);
      Preconditions.checkArgument(column != null, "Cannot find column: %s", name);
      return column;
    }
  }
}
