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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.{Call, DynamicFileFilter, LogicalPlan, OverwriteFiles}
import org.apache.spark.sql.execution.SparkPlan

object ExtendedDataSourceV2Strategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case c @ Call(procedure, args) =>
      CallExec(c.output, procedure.methodHandle, args) :: Nil
    case DynamicFileFilter(scanRelation, fileFilterPlan) =>
      // TODO: add a projection to convert non-columnar execution to unsafe rows?
      // val scanExec = BatchScanExec(scanRelation.output, scanRelation.scan)
      val scanExec = ExtendedBatchScanExec(scanRelation.output, scanRelation.scan)
      DynamicFileFilterExec(scanExec, planLater(fileFilterPlan)) :: Nil
    case OverwriteFiles(_, batchWrite, query) =>
      OverwriteFilesExec(batchWrite, planLater(query)) :: Nil
    case _ => Nil
  }
}
