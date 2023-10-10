package io.hydrolix.connectors

import java.time.Instant

import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connectors.expr.{And, Equal, GetField, Or, StringLiteral, TimestampLiteral, UInt32Literal}
import io.hydrolix.connectors.types.{Int32Type, StringType, TimestampType, UInt32Type}

//noinspection NameBooleanParameters
class PushdownTest {
  private def second(n: Int): Instant = Instant.EPOCH.plusSeconds(n)

  private val unshardedPartitions = List(
    HdxDbPartition("1", second(0), second(60), 1L, 1L, 1L, 1000, 1L, "1", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("2", second(60), second(120), 1L, 1L, 1L, 1000, 1L, "2", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("3", second(120), second(180), 1L, 1L, 1L, 1000, 1L, "3", "42bc986dc5eec4d3", true, None),
    HdxDbPartition("4", second(180), second(240), 1L, 1L, 1L, 1000, 1L, "4", "42bc986dc5eec4d3", true, None),
  )

  private val wyhashAlex = WyHash("Alex")
  private val wyhashBob = WyHash("Bob")
  private val shardedPartitions = List(
    HdxDbPartition("1a", second(0), second(60), 1L, 1L, 1L, 1000, 1L, "1a", wyhashAlex, true, None),
    HdxDbPartition("1b", second(0), second(60), 1L, 1L, 1L, 1000, 1L, "1b", wyhashBob, true, None),
    HdxDbPartition("2a", second(60), second(120), 1L, 1L, 1L, 1000, 1L, "2a", wyhashAlex, true, None),
    HdxDbPartition("2b", second(60), second(120), 1L, 1L, 1L, 1000, 1L, "2b", wyhashBob, true, None),
    HdxDbPartition("3a", second(120), second(180), 1L, 1L, 1L, 1000, 1L, "3a", wyhashAlex, true, None),
    HdxDbPartition("3b", second(120), second(180), 1L, 1L, 1L, 1000, 1L, "3b", wyhashBob, true, None),
    HdxDbPartition("4a", second(180), second(240), 1L, 1L, 1L, 1000, 1L, "4a", wyhashAlex, true, None),
    HdxDbPartition("4b", second(180), second(240), 1L, 1L, 1L, 1000, 1L, "4b", wyhashBob, true, None),
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

  // TODO more tests plz kthx
}
