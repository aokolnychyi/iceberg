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

package org.apache.iceberg.spark.source;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.V2BaseTaskWriter;
import org.apache.iceberg.io.V2ClusteredDataWriter;
import org.apache.iceberg.io.V2TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.source.SparkWrite.UnpartitionedDataWriter;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class SparkUnpartitionedWritersBenchmark {
  private static final Schema SCHEMA = new Schema(
      required(1, "longCol", Types.LongType.get()),
      required(2, "intCol", Types.IntegerType.get()),
      required(3, "floatCol", Types.FloatType.get()),
      optional(4, "doubleCol", Types.DoubleType.get()),
      optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
      optional(6, "dateCol", Types.DateType.get()),
      optional(7, "timestampCol", Types.TimestampType.withZone()),
      required(8, "stringCol", Types.StringType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final Configuration CONF = new Configuration();
  private static final int NUM_RECORDS = 5000000;
  private Iterable<InternalRow> rows;
  private File dataFile;
  private Table table;

  @Setup
  public void setupBenchmark() throws IOException {
    Iterable<InternalRow> generatedRows = RandomData.generateSpark(SCHEMA, NUM_RECORDS, 0L);
    this.rows = Iterables.transform(generatedRows, row -> {
      row.update(7, UTF8String.fromString("a"));
      return row;
    });
    dataFile = File.createTempFile("parquet-flat-data-benchmark", ".parquet");
    dataFile.delete();
    HadoopTables tables = new HadoopTables(CONF);
    Map<String, String> properties = Maps.newHashMap();
    // properties.put(TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true");
    this.table = tables.create(SCHEMA, SPEC, properties, newTableLocation());

  }
  protected String newTableLocation() {
    String tmpDir = CONF.get("hadoop.tmp.dir");
    Path tablePath = new Path(tmpDir, "spark-iceberg-table-" + UUID.randomUUID());
    return tablePath.toString();
  }

  @TearDown
  public void tearDownBenchmark() {
    if (dataFile != null) {
      dataFile.delete();
    }
  }

  @Benchmark
  @Threads(1)
  public void writeUsingOldPartitionedWriter() throws IOException {
    Schema dataSchema = table.schema();
    StructType dataSparkType = SparkSchemaUtil.convert(table.schema());
    SparkAppenderFactory appenderFactory = SparkAppenderFactory.builderFor(table, dataSchema, dataSparkType).build();
    OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1)
        .format(FileFormat.PARQUET)
        .build();
    long targetSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    try (UnpartitionedWriter<InternalRow> closableWriter = new UnpartitionedWriter<>(
        table.spec(), FileFormat.PARQUET, appenderFactory, fileFactory, table.io(), targetSize)) {

      for (InternalRow row : rows) {
        closableWriter.write(row);
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void writeUsingNewPartitionedWriter() throws IOException {
    Schema dataSchema = table.schema();
    StructType dataSparkType = SparkSchemaUtil.convert(table.schema());
    FileFormat fileFormat = FileFormat.PARQUET;
    SparkWriterFactory writerFactory = SparkWriterFactory.builderFor(table)
        .dataSchema(dataSchema)
        .dataSparkType(dataSparkType)
        .dataFileFormat(fileFormat)
        .build();
    OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1)
        .format(fileFormat)
        .build();
    long targetSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    V2ClusteredDataWriter<InternalRow> dataWriter = new V2ClusteredDataWriter<>(
        writerFactory, fileFactory, table.io(),
        fileFormat, targetSize);
    V2TaskWriter<InternalRow> taskWriter = new V2BaseTaskWriter<>(dataWriter, table.io());

    try (UnpartitionedDataWriter writer = new UnpartitionedDataWriter(taskWriter, table.spec())) {
      for (InternalRow row : rows) {
        writer.write(row);
      }
    }
  }
}
