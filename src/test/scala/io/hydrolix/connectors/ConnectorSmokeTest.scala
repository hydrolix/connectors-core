package io.hydrolix.connectors

import java.net.URI
import java.time.Instant
import java.time.temporal.ChronoUnit

import org.junit.{Ignore, Test}

import io.hydrolix.connectors
import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.partitionreader.{CoreRowAdapter, RowPartitionReader}
import io.hydrolix.connectors.types.{StructField, StructType, TimestampType}

class ConnectorSmokeTest {
  @Ignore("Requires environment variables not always available")
  @Test
  def doStuff(): Unit = {
    val jdbcUrl         = System.getenv("HDX_JDBC_URL")
    val apiUrl          = System.getenv("HDX_API_URL")
    val user            = System.getenv("HDX_USER")
    val pass            = System.getenv("HDX_PASSWORD")
    val cloudCred1      = System.getenv("HDX_CLOUD_CRED_1")
    val cloudCred2      = Option(System.getenv("HDX_CLOUD_CRED_2"))
    val dockerImageName = Option(System.getenv("HDX_DOCKER_IMAGE"))

    val info = connectors.HdxConnectionInfo(jdbcUrl, user, pass, new URI(apiUrl), None, cloudCred1, cloudCred2, dockerImageName)

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

    val partitions = HdxPushdown.planPartitions(info, HdxJdbcSession(info), table, StructType(StructField("timestamp", TimestampType(3))), List(pred))

    println(s"${partitions.size} partitions containing data with ${table.primaryKeyField} >= $fiveMinutesAgo")

    val storage = table.storages.getOrElse(partitions.head.storageId, sys.error(s"No storage #${partitions.head.storageId}"))

    println("Timestamp values from first partition:")
    val reader = new RowPartitionReader[StructLiteral](info, storage, "timestamp", partitions.head, CoreRowAdapter, StructLiteral(Map(), StructType()))
    while (reader.next()) {
      val row = reader.get()
      println(row)
      val l = row.getLong(0)
      println(l)
    }
  }
}
