package io.hydrolix.connectors

trait ValueType {
}

trait ScalarType extends ValueType {
}

trait ComplexType extends ValueType {
}

object BooleanType extends ScalarType
object StringType extends ScalarType

object Int8Type extends ScalarType
object Int16Type extends ScalarType
object Int32Type extends ScalarType
object Int64Type extends ScalarType

object Float32Type extends ScalarType
object Float64Type extends ScalarType
case class DecimalType(precision: Int, scale: Int) extends ScalarType
object UInt8Type extends ScalarType
object UInt16Type extends ScalarType
object UInt32Type extends ScalarType
object UInt64Type extends ScalarType {
  // TODO delegate whatever implementation ends up being needed to DecimalType(20,0)
}

/**
 * [[java.time.Instant]] supports nanos in theory, but in practice, on many platforms the three least significant digits
 * are always zeros, so we won't try to promise nanos support
 */
case class TimestampType private (precision: Int) extends ScalarType {
  require(precision >= 0, "Timestamp precision must be >= 0 (seconds) and <= 6 (microseconds)")
  require(precision <= 6, "Implementation limitation: maximum timestamp precision is 6 (microseconds)")
}
object TimestampType {
  val Seconds = TimestampType(0)
  val Millis = TimestampType(3)
  val Micros = TimestampType(6)
}

case class ArrayType(elementType: ValueType, elementsNullable: Boolean = false) extends ComplexType

case class MapType(keyType: ValueType, valueType: ValueType, valuesNullable: Boolean = false) extends ComplexType

case class StructField(name: String, `type`: ValueType, nullable: Boolean = false)

case class StructType(fields: StructField*)
