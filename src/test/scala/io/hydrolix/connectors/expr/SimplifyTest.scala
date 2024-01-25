package io.hydrolix.connectors
package expr

import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connectors.types.{ArrayType, StringType, TimestampType}

//noinspection AssertBetweenInconvertibleTypes,AccessorLikeMethodIsUnit,NameBooleanParameters
class SimplifyTest {
  import PushdownFixture._

  @Test
  def `Basic NOTs`(): Unit = {
    // not(l = r) => [l <> r]
    val orig1 = Not(Equal(getTimestamp, timestampEquals1234))
    assertEquals(NotEqual(getTimestamp, timestampEquals1234), orig1.simplify())

    // not(foo) => not(foo) [unchanged]
    val orig2 = Not(LessThan(getTimestamp, StringLiteral("hello")))
    assertEquals(orig2, orig2.simplify())
  }

  @Test
  def `Basic ANDs`(): Unit = {
    // false AND foo => false; foo AND false => false
    val orig1a = And(BooleanLiteral.False, timestampEquals1234)
    assertEquals(BooleanLiteral.False, orig1a.simplify())
    val orig1b = And(timestampEquals1234, BooleanLiteral.False)
    assertEquals(BooleanLiteral.False, orig1b.simplify())

    // true AND true => true; false AND false => false
    val orig2a = And(BooleanLiteral.True, BooleanLiteral.True)
    assertEquals(BooleanLiteral.True, orig2a.simplify())
    val orig2b = And(BooleanLiteral.False, BooleanLiteral.False)
    assertEquals(BooleanLiteral.False, orig2b.simplify())

    // true AND foo => foo; foo AND true => foo
    val orig3a = And(BooleanLiteral.True, timestampEquals1234)
    assertEquals(timestampEquals1234, orig3a.simplify())
    val orig3b = And(timestampEquals1234, BooleanLiteral.True)
    assertEquals(timestampEquals1234, orig3b.simplify())

    // foo AND foo => foo
    val orig4 = And(timestampEquals1234, timestampEquals1234)
    assertEquals(timestampEquals1234, orig4.simplify())

    // foo AND bar => foo AND bar [unchanged]
    val orig5 = And(timestampEquals1234, timestampEquals2345)
    assertEquals(orig5, orig5.simplify())
  }

  @Test
  def `Basic ORs`(): Unit = {
    // true OR foo => foo; foo OR true => foo
    val orig1a = Or(BooleanLiteral.True, timestampEquals1234)
    assertEquals(BooleanLiteral.True, orig1a.simplify())
    val orig1b = Or(timestampEquals1234, BooleanLiteral.True)
    assertEquals(BooleanLiteral.True, orig1b.simplify())

    // true OR true => true; false OR false => false
    val orig2a = Or(BooleanLiteral.True, BooleanLiteral.True)
    assertEquals(BooleanLiteral.True, orig2a.simplify())
    val orig2b = Or(BooleanLiteral.False, BooleanLiteral.False)
    assertEquals(BooleanLiteral.False, orig2b.simplify())

    // false OR foo => foo; foo OR false => foo
    val orig3a = Or(BooleanLiteral.False, timestampEquals1234)
    assertEquals(timestampEquals1234, orig3a.simplify())
    val orig3b = Or(timestampEquals1234, BooleanLiteral.False)
    assertEquals(timestampEquals1234, orig3b.simplify())

    // foo OR foo => foo
    val orig4 = Or(timestampEquals1234, timestampEquals1234)
    assertEquals(timestampEquals1234, orig4.simplify())

    // foo OR bar => foo OR bar [unchanged]
    val orig5 = Or(timestampEquals1234, timestampEquals2345)
    assertEquals(orig5, orig5.simplify())
  }

  @Test
  def `Basic ==`(): Unit = {
    val orig1 = Equal(getTimestamp, getTimestamp)
    assertEquals(BooleanLiteral.True, orig1.simplify())

    val orig2 = Equal(TimestampLiteral(second(1234)), TimestampLiteral(second(1234)))
    assertEquals(BooleanLiteral.True, orig2.simplify())

    val orig3 = Equal(getTimestamp, TimestampLiteral(second(1234)))
    assertEquals(orig3, orig3.simplify())
  }

  @Test
  def `Basic <>`(): Unit = {
    val orig1 = NotEqual(getTimestamp, getTimestamp)
    assertEquals(BooleanLiteral.False, orig1.simplify())

    val orig2 = NotEqual(TimestampLiteral(second(1234)), TimestampLiteral(second(1234)))
    assertEquals(BooleanLiteral.False, orig2.simplify())

    val orig3 = NotEqual(getTimestamp, TimestampLiteral(second(1234)))
    assertEquals(orig3, orig3.simplify())
  }

  @Test
  def isNull(): Unit = {
    val orig1 = IsNull(StringLiteral(null))
    assertEquals(BooleanLiteral.True, orig1.simplify())
    val orig2 = IsNull(getName)
    assertEquals(orig2, orig2.simplify())
  }

  @Test
  def in(): Unit = {
    // foo IN () => false
    val orig1 = In(getTimestamp, ArrayLiteral(Nil, ArrayType(TimestampType.Millis)))
    assertEquals(BooleanLiteral.False, orig1.simplify())

    // 'foo' IN ('foo', 'bar') => true
    val orig2a = In(StringLiteral("foo"), ArrayLiteral(List("foo", "bar"), ArrayType(StringType, false)))
    assertEquals(BooleanLiteral.True, orig2a.simplify())
    // 'foo' IN ('bar', 'baz') => false
    val orig2b = In(StringLiteral("foo"), ArrayLiteral(List("bar", "baz"), ArrayType(StringType, false)))
    assertEquals(BooleanLiteral.False, orig2b.simplify())

    // non-literal IN <any> => unchanged
    val orig3 = In(getTimestamp, ArrayLiteral(List(Now), ArrayType(TimestampType.Millis, false)))
    assertEquals(orig3, orig3.simplify())
  }
}
