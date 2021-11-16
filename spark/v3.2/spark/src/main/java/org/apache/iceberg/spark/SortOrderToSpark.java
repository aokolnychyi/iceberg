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

package org.apache.iceberg.spark;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.transforms.SortOrderVisitor;
import org.apache.iceberg.util.SortOrderUtil;

class SortOrderToSpark implements SortOrderVisitor<OrderField> {

  private final Map<Integer, String> quotedNameById;

  SortOrderToSpark(SortOrder sortOrder) {
    Set<Integer> sortFieldSourceIds = SortOrderUtil.sortFieldSourceIds(sortOrder);
    this.quotedNameById = SparkSchemaUtil.indexQuotedNameById(sortOrder.schema(), sortFieldSourceIds);
  }

  @Override
  public OrderField field(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return OrderField.column(quotedName(id), direction, nullOrder);
  }

  @Override
  public OrderField bucket(String sourceName, int id, int width, SortDirection direction, NullOrder nullOrder) {
    return OrderField.bucket(quotedName(id), width, direction, nullOrder);
  }

  @Override
  public OrderField truncate(String sourceName, int id, int width, SortDirection direction, NullOrder nullOrder) {
    return OrderField.truncate(quotedName(id), width, direction, nullOrder);
  }

  @Override
  public OrderField year(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return OrderField.year(quotedName(id), direction, nullOrder);
  }

  @Override
  public OrderField month(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return OrderField.month(quotedName(id), direction, nullOrder);
  }

  @Override
  public OrderField day(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return OrderField.day(quotedName(id), direction, nullOrder);
  }

  @Override
  public OrderField hour(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return OrderField.hour(quotedName(id), direction, nullOrder);
  }

  private String quotedName(int id) {
    return quotedNameById.get(id);
  }
}

