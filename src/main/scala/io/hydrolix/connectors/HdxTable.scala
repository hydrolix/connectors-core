package io.hydrolix.connectors

import io.hydrolix.connectors.types.StructType
import io.hydrolix.connectors.expr.Expr

case class HdxTable(info: HdxConnectionInfo,
                 storage: HdxStorageSettings,
                   ident: List[String],
                  schema: StructType,
         primaryKeyField: String,
           shardKeyField: Option[String],
           sortKeyFields: List[String],
                 hdxCols: Map[String, HdxColumnInfo],
               queryMode: HdxQueryMode)

case class HdxPartitionScanPlan(db: String,
                             table: String,
                     partitionPath: String,
                            schema: StructType,
                            pushed: List[Expr[Boolean]],
                           hdxCols: Map[String, HdxColumnInfo])
