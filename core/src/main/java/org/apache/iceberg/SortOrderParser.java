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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

public class SortOrderParser {
  private static final String ORDER_ID = "order-id";
  private static final String FIELDS = "fields";
  private static final String DIRECTION = "direction";
  private static final String NULL_ORDERING = "null-order";
  private static final String FIELD_ID = "id";
  private static final String TRANSFORM = "transform";
  private static final String SOURCE_ID = "source-id";

  private SortOrderParser() {}

  public static void toJson(SortOrder sortOrder, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(ORDER_ID, sortOrder.orderId());
    generator.writeFieldName(FIELDS);
    toJsonFields(sortOrder, generator);
    generator.writeEndObject();
  }

  private static void toJsonFields(SortOrder sortOrder, JsonGenerator generator) throws IOException {
    generator.writeStartArray();
    for (SortField field : sortOrder.fields()) {
      generator.writeStartObject();
      if (field.term() instanceof BoundReference) {
        generator.writeNumberField(FIELD_ID, field.term().ref().fieldId());
      } else if (field.term() instanceof BoundTransform) {
        BoundTransform<?, ?> boundTransform = (BoundTransform<?, ?>) field.term();
        generator.writeStringField(TRANSFORM, boundTransform.transform().toString());
        generator.writeNumberField(SOURCE_ID, boundTransform.ref().fieldId());
      } else {
        throw new RuntimeException("Unsupported sort order term: " + field.term());
      }
      generator.writeStringField(DIRECTION, field.direction().toString().toLowerCase(Locale.ROOT));
      generator.writeStringField(NULL_ORDERING, field.nullOrder().toString().toLowerCase(Locale.ROOT));
      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  public static SortOrder fromJson(Schema schema, JsonNode json) {
    Preconditions.checkArgument(json.isObject(), "Cannot parse sort order from non-object: %s", json);
    int orderId = JsonUtil.getInt(ORDER_ID, json);
    SortOrder.Builder builder = SortOrder.builderFor(schema).withOrderId(orderId);
    buildFromJsonFields(schema, builder, json.get(FIELDS));
    return builder.build();
  }

  private static void buildFromJsonFields(Schema schema, SortOrder.Builder builder, JsonNode json) {
    Preconditions.checkArgument(json.isArray(), "Cannot parse partition order fields, not an array: %s", json);
    Iterator<JsonNode> elements = json.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Preconditions.checkArgument(element.isObject(), "Cannot parse sort field, not an object: %s", element);

      String directionAsString = JsonUtil.getString(DIRECTION, element);
      SortDirection direction = SortDirection.valueOf(directionAsString.toUpperCase(Locale.ROOT));

      String nullOrderingAsString = JsonUtil.getString(NULL_ORDERING, element);
      NullOrder nullOrder = NullOrder.valueOf(nullOrderingAsString.toUpperCase(Locale.ROOT));

      if (element.has(FIELD_ID)) {
        int fieldId = JsonUtil.getInt(FIELD_ID, element);
        Types.NestedField field = schema.findField(fieldId);
        Term term = Expressions.ref(field.name());
        builder.orderBy(term, direction, nullOrder);
      } else if (element.has(TRANSFORM)) {
        int sourceId = JsonUtil.getInt(SOURCE_ID, element);
        Types.NestedField sourceField = schema.findField(sourceId);
        String transformAsString = JsonUtil.getString(TRANSFORM, element);
        Transform<?, ?> transform = Transforms.fromString(sourceField.type(), transformAsString);
        Term term = Expressions.transform(sourceField.name(), transform);
        builder.orderBy(term, direction, nullOrder);
      } else {
        throw new RuntimeException("Invalid sort term: expected either a reference or a transform");
      }
    }
  }
}
