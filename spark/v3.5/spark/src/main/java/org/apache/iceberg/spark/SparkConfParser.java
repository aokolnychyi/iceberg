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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class SparkConfParser {

  private final Map<String, String> properties;
  private final RuntimeConfig sessionConf;
  private final CaseInsensitiveStringMap options;

  SparkConfParser() {
    this.properties = ImmutableMap.of();
    this.sessionConf = new RuntimeConfig(SQLConf.get());
    this.options = CaseInsensitiveStringMap.empty();
  }

  SparkConfParser(SparkSession spark, Table table, Map<String, String> options) {
    this.properties = table.properties();
    this.sessionConf = spark.conf();
    this.options = asCaseInsensitiveStringMap(options);
  }

  public BooleanConfParser booleanConf() {
    return new BooleanConfParser();
  }

  public IntConfParser intConf() {
    return new IntConfParser();
  }

  public LongConfParser longConf() {
    return new LongConfParser();
  }

  public StringConfParser stringConf() {
    return new StringConfParser();
  }

  public DurationConfParser durationConf() {
    return new DurationConfParser();
  }

  public <T extends Enum<T>> EnumConfParser<T> enumConf(Function<String, T> toEnum) {
    return new EnumConfParser<>(toEnum);
  }

  private static CaseInsensitiveStringMap asCaseInsensitiveStringMap(Map<String, String> map) {
    if (map instanceof CaseInsensitiveStringMap) {
      return (CaseInsensitiveStringMap) map;
    } else {
      return new CaseInsensitiveStringMap(map);
    }
  }

  class BooleanConfParser extends ConfParser<BooleanConfParser, Boolean> {
    private Boolean defaultValue;
    private boolean negate = false;

    @Override
    protected BooleanConfParser self() {
      return this;
    }

    public BooleanConfParser defaultValue(boolean value) {
      this.defaultValue = value;
      return self();
    }

    public BooleanConfParser defaultValue(String value) {
      this.defaultValue = Boolean.parseBoolean(value);
      return self();
    }

    public BooleanConfParser negate() {
      this.negate = true;
      return self();
    }

    public boolean parse() {
      boolean value = parseAndCheck(Boolean::parseBoolean, defaultValue);
      return negate ? !value : value;
    }
  }

  class IntConfParser extends ConfParser<IntConfParser, Integer> {
    private Integer defaultValue;

    @Override
    protected IntConfParser self() {
      return this;
    }

    public IntConfParser defaultValue(int value) {
      this.defaultValue = value;
      return self();
    }

    public int parse() {
      return parseAndCheck(Integer::parseInt, defaultValue);
    }

    public Integer parseOptional() {
      return parseOptionalAndCheck(Integer::parseInt, defaultValue);
    }
  }

  class LongConfParser extends ConfParser<LongConfParser, Long> {
    private Long defaultValue;

    @Override
    protected LongConfParser self() {
      return this;
    }

    public LongConfParser defaultValue(long value) {
      this.defaultValue = value;
      return self();
    }

    public long parse() {
      return parseAndCheck(Long::parseLong, defaultValue);
    }

    public Long parseOptional() {
      return parseOptionalAndCheck(Long::parseLong, defaultValue);
    }
  }

  class StringConfParser extends ConfParser<StringConfParser, String> {
    private String defaultValue;

    @Override
    protected StringConfParser self() {
      return this;
    }

    public StringConfParser defaultValue(String value) {
      this.defaultValue = value;
      return self();
    }

    public String parse() {
      return parseAndCheck(Function.identity(), defaultValue);
    }

    public String parseOptional() {
      return parseOptionalAndCheck(Function.identity(), defaultValue);
    }
  }

  class DurationConfParser extends ConfParser<DurationConfParser, Duration> {
    private Duration defaultValue;

    @Override
    protected DurationConfParser self() {
      return this;
    }

    public DurationConfParser defaultValue(Duration value) {
      this.defaultValue = value;
      return self();
    }

    public Duration parse() {
      return parseAndCheck(this::toDuration, defaultValue);
    }

    public Duration parseOptional() {
      return parseOptionalAndCheck(this::toDuration, defaultValue);
    }

    private Duration toDuration(String time) {
      return Duration.ofSeconds(JavaUtils.timeStringAsSec(time));
    }
  }

  class EnumConfParser<T extends Enum<T>> extends ConfParser<EnumConfParser<T>, T> {
    private final Function<String, T> toEnum;
    private T defaultValue;

    EnumConfParser(Function<String, T> toEnum) {
      this.toEnum = toEnum;
    }

    @Override
    protected EnumConfParser<T> self() {
      return this;
    }

    public EnumConfParser<T> defaultValue(T value) {
      this.defaultValue = value;
      return self();
    }

    public EnumConfParser<T> defaultValue(String value) {
      this.defaultValue = toEnum.apply(value);
      return self();
    }

    public T parse() {
      return parseAndCheck(toEnum, defaultValue);
    }

    public T parseOptional() {
      return parseOptionalAndCheck(toEnum, defaultValue);
    }
  }

  abstract class ConfParser<ThisT, T> {
    private final List<String> optionNames = Lists.newArrayList();
    private String sessionConfName;
    private String tablePropertyName;
    private Predicate<T> check;
    private String errMsg;

    protected abstract ThisT self();

    public ThisT option(String name) {
      this.optionNames.add(name);
      return self();
    }

    public ThisT sessionConf(String name) {
      this.sessionConfName = name;
      return self();
    }

    public ThisT tableProperty(String name) {
      this.tablePropertyName = name;
      return self();
    }

    public ThisT check(Predicate<T> newCheck, String newErrMsg) {
      this.check = newCheck;
      this.errMsg = newErrMsg;
      return self();
    }

    protected T parseAndCheck(Function<String, T> conversion, T defaultValue) {
      T value = parse(conversion, defaultValue);
      Preconditions.checkArgument(value != null, "Value cannot be null");
      check(value);
      return value;
    }

    protected T parseOptionalAndCheck(Function<String, T> conversion, T defaultValue) {
      T value = parse(conversion, defaultValue);
      check(value);
      return value;
    }

    private T parse(Function<String, T> conversion, T defaultValue) {
      for (String optionName : optionNames) {
        String optionValue = options.get(optionName);
        if (optionValue != null) {
          return conversion.apply(optionValue);
        }
      }

      if (sessionConfName != null) {
        String sessionConfValue = sessionConf.get(sessionConfName, null);
        if (sessionConfValue != null) {
          return conversion.apply(sessionConfValue);
        }
      }

      if (tablePropertyName != null) {
        String propertyValue = properties.get(tablePropertyName);
        if (propertyValue != null) {
          return conversion.apply(propertyValue);
        }
      }

      return defaultValue;
    }

    private void check(T value) {
      if (check != null && value != null) {
        Preconditions.checkArgument(check.test(value), "Invalid value: %s (%s)", value, errMsg);
      }
    }
  }
}
