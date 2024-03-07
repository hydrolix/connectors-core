package io.hydrolix.connectors

import java.util.UUID

import io.hydrolix.connectors.TestUtils.allColumns
import io.hydrolix.connectors.api.{HdxTransform, HdxTransformSettings, HdxTransformType}
import io.hydrolix.connectors.util.CreateTableAndTransform

object CreateChristmasTreeTable extends App {
  val dbName = args(0)
  val tableName = args(1)

  CreateTableAndTransform(
    HdxConnectionInfo.fromEnv(),
    dbName,
    tableName,
    HdxTransform(
      uuid = UUID.randomUUID(),
      name = s"$dbName.$tableName",
      settings = HdxTransformSettings(
        isDefault = true,
        outputColumns = allColumns
      ),
      `type` = HdxTransformType.json,
      table = uuid0 // needs to be replaced by apply
    )
  )
}
