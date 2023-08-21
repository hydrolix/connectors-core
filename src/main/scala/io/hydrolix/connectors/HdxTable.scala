package io.hydrolix.connectors

import io.substrait.`type`.NamedStruct

case class HdxTable(info: HdxConnectionInfo,
                 storage: HdxStorageSettings,
                   ident: List[String],
                  schema: NamedStruct,
         primaryKeyField: String,
           shardKeyField: Option[String],
           sortKeyFields: List[String],
                 hdxCols: Map[String, HdxColumnInfo],
               queryMode: HdxQueryMode)
