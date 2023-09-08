//noinspection TypeAnnotation
package io.hydrolix.connectors.expr

import java.time.Instant

import io.hydrolix.connectors.types._

trait Literal[+T] extends Expr[T] {
  val value: T
  val `type`: ValueType

  override val children = Nil
}
object Literal {
  def unapply[T](lit: Literal[T]): Option[(T, ValueType)] = {
    Some(lit.value, lit.`type`)
  }
}

case class BooleanLiteral private(value: Boolean) extends Literal[Boolean] {
  override val `type` = BooleanType
}
object BooleanLiteral {
  val True = BooleanLiteral(true)
  val False = BooleanLiteral(false)
}
case class StringLiteral(value: String) extends Literal[String] {
  override val `type` = StringType
}

case class Int8Literal(value: Byte) extends Literal[Byte] {
  override val `type` = Int8Type
}
case class Int16Literal(value: Short) extends Literal[Short] {
  override val `type` = Int16Type
}
case class Int32Literal(value: Int) extends Literal[Int] {
  override val `type` = Int32Type
}
object Int32Literal {
  val `0` = Int32Literal(0)
  val `1` = Int32Literal(1)
}
case class Int64Literal(value: Long) extends Literal[Long] {
  override val `type` = Int64Type
}
object Int64Literal {
  val `0l` = Int64Literal(0L)
  val `1l` = Int64Literal(1L)
}
case class UInt8Literal(value: Short) extends Literal[Short] {
  override val `type` = UInt8Type
}
case class UInt16Literal(value: Int) extends Literal[Int] {
  override val `type` = UInt16Type
}
case class UInt32Literal(value: Long) extends Literal[Long] {
  override val `type` = UInt32Type
}
object UInt32Literal {
  val `0u` = UInt32Literal(0)
  val `1u` = UInt32Literal(1)
}

case class UInt64Literal(value: BigDecimal) extends Literal[BigDecimal] {
  override val `type` = UInt64Type
}
object UInt64Literal {
  val `0lu` = UInt64Literal(0)
  val `1lu` = UInt64Literal(1)
}

case class Float32Literal(value: Float) extends Literal[Float] {
  override val `type` = Float32Type
}
object Float32Literal {
  val `0f` = Float32Literal(0f)
  val `1f` = Float32Literal(1f)
}

case class Float64Literal(value: Double) extends Literal[Double] {
  override val `type` = Float64Type
}
object Float64Literal {
  val `0d` = Float64Literal(0d)
  val `1d` = Float64Literal(1d)
}

/**
 * Note that since this contains an Instant, it declares its type as TimestampType(6) but consumers of the expression
 * may want different precision.
 */
case class TimestampLiteral(value: Instant) extends Literal[Instant] {
  override val `type` = TimestampType(6)
}

case class ArrayLiteral[E](value: List[E], elementType: ValueType, elementsNullable: Boolean = false) extends Literal[List[E]] {
  override val `type` = ArrayType(elementType, elementsNullable)
}

case class MapLiteral[K, V](value: Map[K, V], keyType: ValueType, valueType: ValueType, valuesNullable: Boolean = false) extends Literal[Map[K,V]] {
  override val `type` = MapType(keyType, valueType, valuesNullable)
}

case class StructLiteral(value: Map[String, Any], `type`: StructType) extends Literal[Map[String, Any]]
