package io.hydrolix.connectors

case class HdxTable(info: HdxConnectionInfo,
                 storage: HdxStorageSettings,
                   ident: List[String],
                  schema: StructType,
         primaryKeyField: String,
           shardKeyField: Option[String],
           sortKeyFields: List[String],
                 hdxCols: Map[String, HdxColumnInfo],
               queryMode: HdxQueryMode)
