package io.hydrolix.connectors

import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connectors.expr.{And, BooleanLiteral, Equal, GetField, GreaterEqual, Not, Or, TimestampLiteral}
import io.hydrolix.connectors.types.TimestampType

//noinspection NameBooleanParameters
class PrunablePredicatesTest {
  import PushdownFixture._

  @Test
  def `check prunability of simple predicates`(): Unit = {
    assertEquals("timestamp == literal is prunable", Some(timestampEquals1234), HdxPushdown.prunablePredicate(pkField, None, timestampEquals1234, true))
    assertEquals("(name:string) == 'Alex' IS NOT prunable when shard key is not `name`", None, HdxPushdown.prunablePredicate(pkField, None, nameEqualsAlex, true))
    assertEquals("(name:string) == 'Alex' IS prunable when shard key is `name`", Some(nameEqualsAlex), HdxPushdown.prunablePredicate(pkField, Some(nameField), nameEqualsAlex, true))
    assertEquals("(age:uint32) == 50 is NOT prunable", None, HdxPushdown.prunablePredicate(pkField, Some(nameField), ageEquals50, true))
  }

  @Test
  def `ANDs over prunable predicates are still prunable`(): Unit = {
    val timestampAndTrue = And(timestampEquals1234, BooleanLiteral.True)
    val timestampAndName = And(timestampEquals1234, nameEqualsAlex)
    assertEquals("true is ignored because one leg suffices at top level", Some(timestampAndTrue.left), HdxPushdown.prunablePredicate(pkField, None, timestampAndTrue, true))
    assertEquals("timestamp == literal AND string == literal is pushable", Some(timestampAndName), HdxPushdown.prunablePredicate(pkField, Some(nameField), timestampAndName, true))
  }

  @Test
  def `ORs over uniform prunable predicates are still prunable`(): Unit = {
    val expr = Or(timestampEquals1234, timestampEquals2345)
    assertEquals("timestamp == literal1 OR timestamp == literal2", Some(expr), HdxPushdown.prunablePredicate(pkField, None, expr, true))
  }

  @Test
  def `ORs over non-uniform prunable predicates are not prunable`(): Unit = {
    val expr = Or(timestampEquals1234, nameEqualsAlex)
    assertEquals("timestamp == literal1 OR name == alex", None, HdxPushdown.prunablePredicate(pkField, None, expr, true))
  }

  @Test
  def `AND(OR(...), prunable) still returns prunable`(): Unit = {
    val prunableSubExpr = GreaterEqual(getTimestamp, TimestampLiteral(second(30)))
    val expr = And(
      Or(timestampEquals1234, nameEqualsAlex),
      prunableSubExpr
    )

    assertEquals("AND(OR(...), prunable) still returns prunable", Some(prunableSubExpr), HdxPushdown.prunablePredicate(pkField, None, expr, true))
  }

  @Test
  def `timestamp = not-a-literal isn't prunable`(): Unit = {
    assertEquals(None, HdxPushdown.prunablePredicate(pkField, None, Equal(getTimestamp, GetField("timestamp2", TimestampType.Millis)), true))
  }

  @Test
  def `timestamp = literal AND NOT timestamp = literal`(): Unit = {
    val expr = And(
      timestampEquals1234,
      Not(timestampEquals2345)
    )

    assertEquals(Some(expr), HdxPushdown.prunablePredicate(pkField, None, expr, true))
  }
}
