package io.hydrolix.connectors

import java.util.UUID

import io.hydrolix.connectors.expr.Expr
import io.hydrolix.connectors.types.StructType

case class HdxTable(info: HdxConnectionInfo,
                storages: Map[UUID, HdxStorageSettings],
                   ident: List[String],
                  schema: StructType,
         primaryKeyField: String,
           shardKeyField: Option[String],
           sortKeyFields: List[String],
                 hdxCols: Map[String, HdxColumnInfo],
               queryMode: HdxQueryMode)

case class HdxPartitionScanPlan(db: String,
                             table: String,
                         storageId: UUID,
                     partitionPath: String,
                            schema: StructType,
                            pushed: List[Expr[Boolean]],
                           hdxCols: Map[String, HdxColumnInfo])
