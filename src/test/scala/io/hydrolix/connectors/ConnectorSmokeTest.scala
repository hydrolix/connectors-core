package io.hydrolix.connectors

import java.time.Instant
import java.time.temporal.ChronoUnit

import org.junit.{Ignore, Test}

import io.hydrolix.connectors.TestUtils.connectionInfo
import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.partitionreader.{CoreRowAdapter, RowPartitionReader}
import io.hydrolix.connectors.types.{StructField, StructType, TimestampType}

class ConnectorSmokeTest {
  @Ignore("Requires environment variables not always available")
  @Test
  def listTransforms(): Unit = {
    val info: HdxConnectionInfo = connectionInfo()
    val api = new HdxApiSession(info)
    val transforms = api.transforms("hydro", "logs")
    println(transforms)
  }

  @Ignore("Requires environment variables not always available")
  @Test
  def doStuff(): Unit = {
    val info = connectionInfo()

    val catalog = new HdxTableCatalog()
    catalog.initialize("hdx-test", info.asMap)

    val table = catalog.loadTable(List("hydro", "logs"))

    val now = Instant.now()
    val fiveMinutesAgo = now.minus(5L, ChronoUnit.MINUTES)

    val getTimestamp = expr.GetField(table.primaryKeyField, TimestampType(3))

    val pred = And(List(
      GreaterEqual(getTimestamp, TimestampLiteral(fiveMinutesAgo)),
      LessEqual(getTimestamp, TimestampLiteral(now))
    ))

    val partitions = HdxPushdown.planPartitions(info, HdxJdbcSession(info), table, StructType(List(StructField("timestamp", TimestampType(3)))), List(pred))

    println(s"${partitions.size} partitions containing data with ${table.primaryKeyField} >= $fiveMinutesAgo")

    val storage = table.storages.getOrElse(partitions.head.storageId, sys.error(s"No storage #${partitions.head.storageId}"))

    println("Timestamp values from first partition:")
    val reader = new RowPartitionReader[StructLiteral](info, storage, "timestamp", partitions.head, CoreRowAdapter, StructLiteral(Map(), StructType(Nil)))
    while (reader.next()) {
      val row = reader.get()
      println(row)
      val l = row.getLong(0)
      println(l)
    }
  }
}
