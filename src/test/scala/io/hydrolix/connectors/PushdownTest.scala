package io.hydrolix.connectors

import java.io.File
import java.net.URI
import java.sql
import java.sql.PreparedStatement
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.UUID

import org.h2.jdbcx.JdbcDataSource
import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.types.{Int32Type, StringType, TimestampType, UInt32Type}

//noinspection NameBooleanParameters
class PushdownTest {
  private def second(n: Int): Instant = Instant.EPOCH.plusSeconds(n)

  private val unshardedPartitions = List(
    HdxDbPartition("1", second(0), second(60), 1L, 1L, 1L, 1000, 1L, "1", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("2", second(30), second(90), 1L, 1L, 1L, 1000, 1L, "2", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("3", second(60), second(120), 1L, 1L, 1L, 1000, 1L, "3", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("4", second(90), second(150), 1L, 1L, 1L, 1000, 1L, "4", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("5", second(120), second(180), 1L, 1L, 1L, 1000, 1L, "5", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("6", second(150), second(210), 1L, 1L, 1L, 1000, 1L, "6", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("7", second(180), second(240), 1L, 1L, 1L, 1000, 1L, "7", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("8", second(210), second(270), 1L, 1L, 1L, 1000, 1L, "8", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("9", second(240), second(300), 1L, 1L, 1L, 1000, 1L, "9", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("10", second(270), second(330), 1L, 1L, 1L, 1000, 1L, "10", "42bc986dc5eec4d3", true, None),
  )

  // TODO write some tests for sharded partitions too
  // TODO write some tests for sharded partitions too
  // TODO write some tests for sharded partitions too
  // TODO write some tests for sharded partitions too
  private val wyhashAlex = WyHash("Alex")
  private val wyhashBob = WyHash("Bob")
  private val shardedPartitions = List(
    HdxDbPartition("1a", second(0), second(60), 1L, 1L, 1L, 1000, 1L, "1a", wyhashAlex, true, None),
    HdxDbPartition("1b", second(0), second(60), 1L, 1L, 1L, 1000, 1L, "1b", wyhashBob, true, None),
    HdxDbPartition("2a", second(30), second(90), 1L, 1L, 1L, 1000, 1L, "2a", wyhashAlex, true, None),
    HdxDbPartition("2b", second(30), second(90), 1L, 1L, 1L, 1000, 1L, "2b", wyhashBob, true, None),
    HdxDbPartition("3a", second(60), second(120), 1L, 1L, 1L, 1000, 1L, "3a", wyhashAlex, true, None),
    HdxDbPartition("3b", second(60), second(120), 1L, 1L, 1L, 1000, 1L, "3b", wyhashBob, true, None),
    HdxDbPartition("4a", second(90), second(150), 1L, 1L, 1L, 1000, 1L, "4a", wyhashAlex, true, None),
    HdxDbPartition("4b", second(90), second(150), 1L, 1L, 1L, 1000, 1L, "4b", wyhashBob, true, None),
    HdxDbPartition("5a", second(120), second(180), 1L, 1L, 1L, 1000, 1L, "5a", wyhashAlex, true, None),
    HdxDbPartition("5b", second(120), second(180), 1L, 1L, 1L, 1000, 1L, "5b", wyhashBob, true, None),
    HdxDbPartition("6a", second(150), second(210), 1L, 1L, 1L, 1000, 1L, "6a", wyhashAlex, true, None),
    HdxDbPartition("6b", second(150), second(210), 1L, 1L, 1L, 1000, 1L, "6b", wyhashBob, true, None),
    HdxDbPartition("7a", second(180), second(240), 1L, 1L, 1L, 1000, 1L, "7a", wyhashAlex, true, None),
    HdxDbPartition("7b", second(180), second(240), 1L, 1L, 1L, 1000, 1L, "7b", wyhashBob, true, None),
    HdxDbPartition("8a", second(210), second(270), 1L, 1L, 1L, 1000, 1L, "8a", wyhashAlex, true, None),
    HdxDbPartition("8b", second(210), second(270), 1L, 1L, 1L, 1000, 1L, "8b", wyhashBob, true, None),
    HdxDbPartition("9a", second(240), second(300), 1L, 1L, 1L, 1000, 1L, "9a", wyhashAlex, true, None),
    HdxDbPartition("9b", second(240), second(300), 1L, 1L, 1L, 1000, 1L, "9b", wyhashBob, true, None),
    HdxDbPartition("10a", second(270), second(330), 1L, 1L, 1L, 1000, 1L, "10a", wyhashAlex, true, None),
    HdxDbPartition("10b", second(270), second(330), 1L, 1L, 1L, 1000, 1L, "10b", wyhashBob, true, None),
  )

  private val pkField = "timestamp"
  private val nameField = "name"
  private val ageField = "age"

  private val cols = Map(
    pkField -> HdxColumnInfo(pkField, Types.valueTypeToHdx(TimestampType.Millis).copy(primary = true), false, TimestampType.Millis, 2),
    nameField -> HdxColumnInfo(nameField, HdxColumnDatatype(HdxValueType.String, false, false), true, StringType, 2),
    ageField -> HdxColumnInfo(ageField, HdxColumnDatatype(HdxValueType.UInt32, true, false), true, UInt32Type, 2),
  )

  val getTimestamp = GetField(pkField, TimestampType.Millis)
  val getName = GetField(nameField, StringType)
  val getAge = GetField(ageField, Int32Type)
  val timestampEquals1234 = Equal(getTimestamp, TimestampLiteral(second(1234)))
  val timestampEquals2345 = Equal(getTimestamp, TimestampLiteral(second(2345)))
  val nameEqualsAlex = Equal(getName, StringLiteral("Alex"))
  val ageEquals50 = Equal(getAge, UInt32Literal(50))

  @Test
  def `check pushability of simple predicates`(): Unit = {
    assertEquals("timestamp == literal is pushable", 2, HdxPushdown.pushable(pkField, None, timestampEquals1234, cols))
    assertEquals("(name:string) == 'Alex' is pushable", 2, HdxPushdown.pushable(pkField, None, nameEqualsAlex, cols))
    assertEquals("(age:uint32) == 50 is NOT pushable", 3, HdxPushdown.pushable(pkField, None, ageEquals50, cols))
  }

  @Test
  def `ANDs over pushable predicates are still pushable`(): Unit = {
    assertEquals("timestamp == literal AND nothing", 2, HdxPushdown.pushable(pkField, None, And(List(timestampEquals1234)), cols))
    assertEquals("timestamp == literal AND string == literal is pushable", 2, HdxPushdown.pushable(pkField, None, And(List(timestampEquals1234, nameEqualsAlex)), cols))
  }

  @Test
  def `ORs over uniform pushable predicates are still pushable`(): Unit = {
    assertEquals("timestamp == literal1 OR timestamp == literal2", 2, HdxPushdown.pushable(pkField, None, Or(List(timestampEquals1234, timestampEquals2345)), cols))
  }

  @Test
  def `get all partitions when no bounds`(): Unit = {
    val parts = queryUnsharded(None, None)

    assertEquals(unshardedPartitions.size, parts.size)
  }

  @Test
  def `get all partitions when min & max bounds overlap everything`(): Unit = {
    val parts = queryUnsharded(Some(second(30)), Some(second(330)))

    assertEquals(unshardedPartitions.size, parts.size)
  }

  @Test
  def `get all partitions when min & max bounds exactly match partitions`(): Unit = {
    val parts = queryUnsharded(Some(second(0)), Some(second(360)))

    assertEquals(unshardedPartitions.size, parts.size)
  }

  @Test
  def `get all but two partitions when min & max bounds are just inside`(): Unit = {
    val parts = queryUnsharded(
      Some(unshardedPartitions.head.maxTimestamp.plusSeconds(1)),
      Some(unshardedPartitions.last.minTimestamp.minusSeconds(1))
    )

    assertEquals(unshardedPartitions.size - 2, parts.size)
  }

  private def queryUnsharded(min: Option[Instant], max: Option[Instant]) = {
    val (jdbc, ps) = setupCatalog()

    insertPartitions(ps, unshardedPartitions)

    jdbc.collectPartitions("testdb", "testtable", min, max)
  }

  private def insertPartitions(ps: PreparedStatement, partitions: List[HdxDbPartition]): Unit = {
    for (p <- partitions) {
      ps.clearParameters()
      ps.setString(1, p.partition)
      ps.setObject(2, LocalDateTime.ofInstant(p.minTimestamp, ZoneId.of("UTC")))
      ps.setObject(3, LocalDateTime.ofInstant(p.maxTimestamp, ZoneId.of("UTC")))
      ps.setLong(4, p.manifestSize)
      ps.setLong(5, p.dataSize)
      ps.setLong(6, p.indexSize)
      ps.setLong(7, p.rows)
      ps.setLong(8, p.memSize)
      ps.setString(9, p.rootPath)
      ps.setString(10, p.shardKey)
      ps.setByte(11, if (p.active) 1 else 0)

      p.storageId match {
        case Some(id) => ps.setString(12, id.toString)
        case None => ps.setNull(12, sql.Types.VARCHAR)
      }

      ps.executeUpdate()
    }
  }

  private def setupCatalog(): (HdxJdbcSession, PreparedStatement) = {
    val tmp = new File(System.getenv("java.io.tmpdir"), s"testdb-${UUID.randomUUID().toString}")
    tmp.mkdirs()
    new RmRfThread(tmp).hook()

    val url = s"jdbc:h2:file:${tmp.getAbsolutePath}"

    val ds = new JdbcDataSource().also(_.setURL(url))
    val conn = ds.getConnection
    val stmt = conn.createStatement()
    stmt.executeUpdate("""CREATE SCHEMA IF NOT EXISTS testdb""")
    stmt.executeUpdate(
      s"""CREATE TABLE testdb.`testtable#.catalog` (
         |  partition VARCHAR(128) NOT NULL,
         |  min_timestamp TIMESTAMP NOT NULL,
         |  max_timestamp TIMESTAMP NOT NULL,
         |  manifest_size BIGINT NOT NULL,
         |  data_size BIGINT NOT NULL,
         |  index_size BIGINT NOT NULL,
         |  rows BIGINT NOT NULL,
         |  mem_size BIGINT NOT NULL,
         |  root_path VARCHAR(128) NOT NULL,
         |  shard_key VARCHAR(24) NOT NULL,
         |  active TINYINT NOT NULL,
         |  storage_id VARCHAR(36)
         |)""".stripMargin)

    val ps = conn.prepareStatement(
      """INSERT INTO testdb.`testtable#.catalog` (
        |  partition,
        |  min_timestamp,
        |  max_timestamp,
        |  manifest_size,
        |  data_size,
        |  index_size,
        |  rows,
        |  mem_size,
        |  root_path,
        |  shard_key,
        |  active,
        |  storage_id
        |) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""".stripMargin.replace("\n", ""))

    val jdbc = HdxJdbcSession(HdxConnectionInfo(
      ds.getUrl,
      "",
      "",
      new URI("https://hdx.example.com/config/v1/"),
      None,
      "hello",
      Some("goodbye"),
      None,
      Some(ds)
    ))

    (jdbc, ps)
  }
}
