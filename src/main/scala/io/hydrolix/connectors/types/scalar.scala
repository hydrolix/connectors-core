package io.hydrolix.connectors.types

import scala.collection.mutable

sealed abstract class ScalarType(val decl: String) extends ValueType {
  ScalarType.byName(decl) = this
}

object ScalarType {
  var byName = mutable.Map[String, ScalarType]()
}

object BooleanType extends ScalarType("boolean")
object StringType extends ScalarType("string")

object Int8Type extends ScalarType("int8")
object Int16Type extends ScalarType("int16")
object Int32Type extends ScalarType("int32")
object Int64Type extends ScalarType("int64")
object UInt8Type extends ScalarType("uint8")
object UInt16Type extends ScalarType("uint16")
object UInt32Type extends ScalarType("uint32")
object UInt64Type extends ScalarType("uint64") {
  // TODO delegate whatever implementation ends up being needed to DecimalType(20,0)
}

object Float32Type extends ScalarType("float32")
object Float64Type extends ScalarType("float64")
case class DecimalType(precision: Int, scale: Int) extends ScalarType(s"decimal($precision,$scale)")

/**
 * [[java.time.Instant]] supports nanos in theory, but in practice, on many platforms the three least significant digits
 * are always zeros, so we won't try to promise nanos support
 */
case class TimestampType private (precision: Int) extends ScalarType(s"timestamp($precision)") {
  require(precision >= 0, "Timestamp precision must be >= 0 (seconds) and <= 6 (microseconds)")
  require(precision <= 6, "Implementation limitation: maximum timestamp precision is 6 (microseconds)")
}
object TimestampType {
  val Seconds = TimestampType(0)
  val Millis = TimestampType(3)
  val Micros = TimestampType(6)
}
