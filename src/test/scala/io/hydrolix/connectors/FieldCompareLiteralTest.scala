package io.hydrolix.connectors

import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connectors.FieldCompareLiteral.SpecificField
import io.hydrolix.connectors.expr.{ComparisonOp, Equal, TimestampLiteral}
import io.hydrolix.connectors.types.{BooleanType, TimestampType}

class FieldCompareLiteralTest {

  import PushdownFixture._

  @Test
  def testFCLSameType(): Unit = {
    val timestampCompare = FieldCompareLiteral(Some(SpecificField(pkField)), TimestampType.Millis)
    val xx = timestampCompare.unapply(Equal(getTimestamp, TimestampLiteral(second(123))))
    assertEquals(Some(pkField, ComparisonOp.EQ, second(123)), xx)
  }

  @Test
  def testFCLCast(): Unit = {
    val timestampCompare = FieldCompareLiteral(Some(SpecificField(pkField)), TimestampType.Micros)
    val xx = timestampCompare.unapply(Equal(getTimestamp, TimestampLiteral(second(123))))
    assertEquals(Some(pkField, ComparisonOp.EQ, second(123)), xx)
  }

  @Test
  def testFCLCastFailure(): Unit = {
    val badCompare = FieldCompareLiteral(Some(SpecificField(pkField)), BooleanType)
    val xx = badCompare.unapply(Equal(getTimestamp, TimestampLiteral(second(123))))
    assertEquals(None, xx)
  }
}
