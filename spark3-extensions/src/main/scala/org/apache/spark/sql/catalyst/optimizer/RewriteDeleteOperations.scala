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

package org.apache.spark.sql.catalyst.optimizer

import java.util.UUID
import org.apache.spark.sql.{sources, AnalysisException}
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EqualNullSafe, Expression, InputFileName, Literal, Not, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DeleteFromTable, DynamicFileFilter, Filter, LocalRelation, LogicalPlan, OverwriteFiles, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{SupportsMetadataOnlyDeletes, Table}
import org.apache.spark.sql.connector.write.{CopyOnWriteOperationsBuilder, LogicalWriteInfo, LogicalWriteInfoImpl}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

// TODO: should be part of early scan push down after the delete condition is optimized
object RewriteDeleteOperations extends Rule[LogicalPlan] with PredicateHelper {

  import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // no need to execute this delete as the condition is false
    case DeleteFromTable(_, Some(FalseLiteral)) =>
      LocalRelation()

    // don't rewrite deletes that can be answered using metadata only operations
    case d @ DeleteFromTable(r: DataSourceV2Relation, Some(cond)) if isMetadataOnlyOperation(r, cond) =>
      d

    // rewrite all operations that require reading the table to delete records
    case DeleteFromTable(r: DataSourceV2Relation, Some(cond)) =>
      val writeInfo = newWriteInfo(r.schema)
      val opsBuilder = r.table.asRowLevelModifiable.newRowLevelOperationsBuilder(writeInfo)
      opsBuilder match {
        case builder: CopyOnWriteOperationsBuilder =>
          val predicates = splitConjunctivePredicates(cond)
          val dataSourceFilters = toDataSourceFilters(predicates, r.output)
          builder.pushDownOverwriteFilters(dataSourceFilters)

          val scan = builder.buildScan()
          val scanRelation = DataSourceV2ScanRelation(r.table, scan, r.output)

          // TODO: do we need to wrap back into subquery aliases?
          val fileNameExpr = Alias(InputFileName(), "_file")()
          val projection = Project(Seq(fileNameExpr), Filter(cond, scanRelation))
          // we have to use Aggregate directly as this rule is executed
          // after the optimizer replaces Deduplicate and Distinct nodes
          val fileFilterPlan = Aggregate(projection.output, projection.output, projection)
          val dynamicFileFilter = DynamicFileFilter(scanRelation, fileFilterPlan)

          val batchWrite = builder.buildBatchWrite()
          val rowFilter = Not(EqualNullSafe(cond, Literal(true, BooleanType)))
          // TODO: group by something so that we can write back
          // either local sort by partition values or just add optional repartition before writing
          // or repartition by _file and local sort by partition value
          OverwriteFiles(r, batchWrite, Filter(rowFilter, dynamicFileFilter))

        case _ =>
          throw new AnalysisException("Unsupported mode for row-level operations")
      }
  }

  private def isMetadataOnlyOperation(relation: DataSourceV2Relation, cond: Expression): Boolean = {
    relation.table match {
      case t: SupportsMetadataOnlyDeletes if isMetadataOnlyCondition(t, cond) =>
        val predicates = splitConjunctivePredicates(cond)
        val dataSourceFilters = toDataSourceFilters(predicates, relation.output)
        t.canDeleteUsingMetadataWhere(dataSourceFilters)
      case _ => false
    }
  }

  private def isMetadataOnlyCondition(table: Table, cond: Expression): Boolean = {
    val partitionColumnRefs = table.partitioning.flatMap(_.references)
    val partitionColumnNames = partitionColumnRefs.map(_.fieldNames.mkString(".")).toSet

    val resolver = SQLConf.get.resolver
    val predicates = splitConjunctivePredicates(cond)

    predicates.forall { pred =>
      val isExecutable = pred.deterministic && !SubqueryExpression.hasSubquery(pred)
      isExecutable && isMetadataPredicate(pred, partitionColumnNames, resolver)
    }
  }

  private def isMetadataPredicate(
      predicate: Expression,
      partitionColumnNames: Set[String],
      resolver: Resolver): Boolean = {
    predicate.references.forall { reference => partitionColumnNames.exists(resolver(reference.name, _)) }
  }

  private def newWriteInfo(schema: StructType): LogicalWriteInfo = {
    val uuid = UUID.randomUUID()
    LogicalWriteInfoImpl(queryId = uuid.toString, schema, CaseInsensitiveStringMap.empty)
  }

  private def toDataSourceFilters(
      predicates: Seq[Expression],
      output: Seq[AttributeReference],
      skipUntranslated: Boolean = true): Array[sources.Filter] = {

    val normalizedPredicates = DataSourceStrategy.normalizeExprs(predicates, output)
    normalizedPredicates.flatMap { p =>
      val translatedFilter = DataSourceStrategy.translateFilter(p, supportNestedPredicatePushdown = true)
      if (translatedFilter.isEmpty && !skipUntranslated) {
        throw new AnalysisException(s"Cannot translate expression to source filter: $p")
      }
      translatedFilter
    }.toArray
  }
}
