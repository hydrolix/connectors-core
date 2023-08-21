package io.hydrolix.connectors

import java.time.{Instant, ZoneOffset}
import java.util.{Properties, UUID}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Using}

import com.clickhouse.jdbc.ClickHouseDataSource
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

object HdxJdbcSession {
  private val cache = mutable.Map[HdxConnectionInfo, HdxJdbcSession]()

  def apply(info: HdxConnectionInfo): HdxJdbcSession = {
    cache.getOrElse(info, {
      val fresh = new HdxJdbcSession(info)
      cache += (info -> fresh)
      fresh
    })
  }
}

/**
 * TODO this uses a single connection for metadata about all databases; maybe there should be one of these per DB
 */
class HdxJdbcSession private (info: HdxConnectionInfo) {
  private lazy val pool = {
    val ds = {
      val props = new Properties()
      props.put("web_context", "/query")
      props.put("path", "/query")
      props.put("user", info.user)
      props.put("username", info.user)
      props.put("password", info.password)
      props.put("compress", "false")
      props.put("ssl", "true")

      new ClickHouseDataSource(info.jdbcUrl, props)
    }

    val props = new Properties()
    props.put("jdbcUrl", info.jdbcUrl)
    props.put("dataSource", ds)
    new HikariDataSource(new HikariConfig(props))
  }

  /**
   * Get the sum(rows), min(primary) and max(primary) of ALL partitions
   */
  def collectPartitionAggs(db: String, table: String): (Long, Instant, Instant) = {
    Using.Manager { use =>
      val conn = use(pool.getConnection)
      val stmt = use(conn.createStatement())
      val rs = use(stmt.executeQuery(
        s"""SELECT
           |  sum(rows) as rows,
           |  min(min_timestamp) as min_primary,
           |  max(max_timestamp) as max_primary
           |FROM `$db`.`$table#.catalog`""".stripMargin))

      rs.next()

      (
        rs.getLong("rows"),
        rs.getTimestamp("min_primary").toLocalDateTime.toInstant(ZoneOffset.UTC),
        rs.getTimestamp("max_primary").toLocalDateTime.toInstant(ZoneOffset.UTC)
      )
    }.get
  }

  def collectPartitions(db: String, table: String): List[HdxDbPartition] = {
    Using.Manager { use =>
      val conn = use(pool.getConnection)
      val stmt = use(conn.createStatement())
      val rs = use(stmt.executeQuery(s"SELECT * FROM `$db`.`$table#.catalog`"))

      val partitions = ListBuffer[HdxDbPartition]()

      val hasStorageId = Try(rs.findColumn("storage_id")).isSuccess

      while (rs.next()) {
        partitions += HdxDbPartition(
          rs.getString("partition"),
          rs.getTimestamp("min_timestamp").toLocalDateTime.toInstant(ZoneOffset.UTC),
          rs.getTimestamp("max_timestamp").toLocalDateTime.toInstant(ZoneOffset.UTC),
          rs.getLong("manifest_size"),
          rs.getLong("data_size"),
          rs.getLong("index_size"),
          rs.getLong("rows"),
          rs.getLong("mem_size"),
          rs.getString("root_path"),
          rs.getString("shard_key"),
          rs.getByte("active") == 1,
          if (hasStorageId) {
            rs.getString("storage_id").noneIfEmpty.map(UUID.fromString)
          } else {
            None
          }
        )
      }
      partitions.toList
    }.get
  }
}

