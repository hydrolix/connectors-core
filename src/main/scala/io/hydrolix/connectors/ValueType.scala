package io.hydrolix.connectors

import java.time.Instant

trait ValueType[T <: Any] {
}

trait ScalarType[T <: Any] extends ValueType[T] {
}

trait ComplexType[T <: Any] extends ValueType[T] {
}

object BooleanType extends ScalarType[Boolean]
object StringType extends ScalarType[String]

object Int8Type extends ScalarType[Byte]
object Int16Type extends ScalarType[Short]
object Int32Type extends ScalarType[Int]
object Int64Type extends ScalarType[Long]

object Float32Type extends ScalarType[Float]
object Float64Type extends ScalarType[Double]
case class DecimalType(precision: Int, scale: Int) extends ScalarType[BigDecimal]
object UInt8Type extends ScalarType[Short]
object UInt16Type extends ScalarType[Int]
object UInt32Type extends ScalarType[Long]
object UInt64Type extends ScalarType[BigDecimal] {
  // TODO delegate whatever implementation ends up being needed to DecimalType(20,0)
}

case class TimestampType private (precision: Int) extends ScalarType[Instant] {
  require(precision >= 0, "Timestamp precision must be >= 0 (seconds) and <= 6 (microseconds)")
  require(precision <= 6, "Implementation limitation: maximum timestamp precision is 6 (microseconds)")
}
object TimestampType {
  val Seconds = TimestampType(0)
  val Millis = TimestampType(3)
  val Micros = TimestampType(6)
}

case class ArrayType[E <: Any](elementType: ValueType[E], 
                          elementsNullable: Boolean = false) 
  extends ComplexType[List[E]] 

case class MapType[K <: Any, V <: Any](keyType: ValueType[K], 
                                     valueType: ValueType[V], 
                                valuesNullable: Boolean = false)
  extends ComplexType[Map[K,V]]

case class StructField[T <: Any](name: String, 
                               `type`: ValueType[T], 
                             nullable: Boolean = false)

case class StructType(fields: StructField[_]*)
