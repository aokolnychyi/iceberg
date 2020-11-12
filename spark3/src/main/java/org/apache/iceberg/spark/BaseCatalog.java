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

import java.util.Locale;
import org.apache.iceberg.spark.procedures.SparkProcedure;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.iceberg.catalog.Procedure;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureCatalog;

abstract class BaseCatalog implements StagingTableCatalog, ProcedureCatalog, SupportsNamespaces {

  @Override
  public Procedure loadProcedure(Identifier ident) throws NoSuchProcedureException {
    String[] namespace = ident.namespace();
    String name = ident.name();

    // namespace resolution is case sensitive to match how we resolve namespaces for tables right now
    if (namespace.length == 1 && namespace[0].equals("system")) {
      try {
        // procedure resolution is case insensitive to match the existing Spark behavior for functions
        // SimpleFunctionRegistry normalizes function names but leaves namespace resolution to the caller
        SparkProcedure procedure = SparkProcedure.valueOf(name.toUpperCase(Locale.ROOT));
        return procedure.build(this);
      } catch (IllegalArgumentException e) {
        throw new NoSuchProcedureException(ident);
      }
    }

    throw new NoSuchProcedureException(ident);
  }
}
