/*
 * Copyright (c) 2023 Hydrolix Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hydrolix.connectors

import java.sql.PreparedStatement
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}
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
    val ds = info.dataSource.getOrElse {
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

  def collectPartitions(db: String,
                     table: String,
                  earliest: Option[Instant],
                    latest: Option[Instant],
            shardKeyHashes: Set[String])
                          : List[HdxDbPartition] =
  {
    val (query, setParams) = catalogQuery(db, table, earliest, latest, shardKeyHashes)

    Using.Manager { use =>
      val conn = use(pool.getConnection)
      val stmt = use(conn.prepareStatement(query))
      setParams(stmt)

      val rs = use(stmt.executeQuery())

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
            val sid = rs.getString("storage_id")
            if (rs.wasNull()) {
              None
            } else {
              sid.noneIfEmpty.map(UUID.fromString)
            }
          } else {
            None
          }
        )
      }
      partitions.toList
    }.get
  }

  def catalogQuery(db: String,
                table: String,
             earliest: Option[Instant],
               latest: Option[Instant],
       shardKeyHashes: Set[String])
                     : (String, PreparedStatement => Unit) =
  {
    val prefix = s"SELECT * FROM `$db`.`$table#.catalog` p"
    val parse = info.timestampLiteralConv.getOrElse("parseDateTimeBestEffort(?)")

    val (hashClause, hashFunc) = if (shardKeyHashes.isEmpty) {
      (None, (_: PreparedStatement, _: Int) => ())
    } else {
      val qs = List.fill(shardKeyHashes.size)("?").mkString(",")

      (
        Some(s"p.shard_key IN ($qs)"),
        { (stmt: PreparedStatement, startPos: Int) =>
          for ((hash, i) <- shardKeyHashes.zipWithIndex) {
            stmt.setString(startPos + i, hash)
          }
        }
      )
    }

    val (timeClause, timeFunc, nextParamPos) = (earliest, latest) match {
      // Partition max is >= query min, and partition min is <= query max
      case (Some(qmin), Some(qmax)) =>
        if (qmax.isBefore(qmin)) sys.error(s"Query max timestamp $qmax was before min $qmin!")

        (
          Some(s"p.max_timestamp >= $parse AND p.min_timestamp <= $parse"),
          { (stmt: PreparedStatement) =>
            stmt.setObject(1, LocalDateTime.ofInstant(qmin, ZoneId.of("UTC")))
            stmt.setObject(2, LocalDateTime.ofInstant(qmax, ZoneId.of("UTC")))
          },
          3
        )
      case (Some(qmin), None) =>
        // Partition max is >= query min
        (
          Some(s"p.max_timestamp >= $parse"),
          { stmt: PreparedStatement =>
            stmt.setObject(1, LocalDateTime.ofInstant(qmin, ZoneId.of("UTC")))
          },
          2
        )
      case (None, Some(qmax)) =>
        // Partition min is <= query max
        (
          Some(s"p.min_timestamp <= $parse"),
          { stmt: PreparedStatement =>
            stmt.setObject(1, LocalDateTime.ofInstant(qmax, ZoneId.of("UTC")))
          },
          2
        )
      case (None, None) =>
        // Unconstrained
        (
          None,
          { _: PreparedStatement => () },
          1
        )
    }

    val clauses = timeClause.toList ++ hashClause.toList

    val where = if (clauses.isEmpty) "" else clauses.mkString(" WHERE (", ") AND (", ")")

    (
      prefix + where,
      { stmt: PreparedStatement =>
        timeFunc(stmt)
        hashFunc(stmt, nextParamPos)
      }
    )
  }
}
