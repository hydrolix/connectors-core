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

package io.hydrolix.connectors.types

import java.math.BigInteger
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.{math => jm}
import scala.collection.mutable
import scala.math.Ordered.orderingToOrdered

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._

import io.hydrolix.connectors

sealed abstract class ScalarType(val decl: String) extends ConcreteType {
  ScalarType.byName(decl) = this
  def failRange(node: JsonNode): Either[String, T] = Left(s"Node $node out of range for $decl")
}

object ScalarType {
  var byName = mutable.Map[String, ScalarType]()
}

object BooleanType extends ScalarType("boolean") {
  override val toString = "BooleanType"

  override type T = Boolean

  override def toJson(value: Boolean): JsonNode = BooleanNode.valueOf(value)
  override def fromJson(node: JsonNode): Either[String, Boolean] = {
    node match {
      case b: BooleanNode => Right(b.booleanValue())
      case t: TextNode => Right(t.booleanValue())
      case _ => fail(node)
    }
  }
}

object StringType extends ScalarType("string") {
  override type T = String

  override def toJson(value: String): JsonNode = TextNode.valueOf(value)
  override def fromJson(node: JsonNode): Either[String, String] = {
    node match {
      case t: TextNode => Right(t.textValue())
      case _ => fail(node) // TODO maybe toString is good sometimes
    }
  }
  override val toString = "StringType"
}

object Int8Type extends ScalarType("int8") {
  override val toString = "Int8Type"

  override type T = Byte

  override def toJson(value: Byte): JsonNode = ShortNode.valueOf(value)
  override def fromJson(node: JsonNode): Either[String, Byte] = {
    node match {
      case n: NumericNode if n.shortValue() >= Byte.MinValue && n.shortValue() <= Byte.MaxValue => Right(n.shortValue().toByte)
      case _: NumericNode => failRange(node)
      case _ => fail(node) // TODO maybe convert other kinds of node?
    }
  }
}

object Int16Type extends ScalarType("int16") {
  override val toString = "Int16Type"

  override type T = Short

  override def toJson(value: Short): JsonNode = ShortNode.valueOf(value)
  override def fromJson(node: JsonNode): Either[String, Short] = {
    node match {
      case n: NumericNode if n.intValue() >= Short.MinValue && n.intValue() <= Short.MaxValue => Right(n.shortValue())
      case _: NumericNode => failRange(node)
      case _ => fail(node) // TODO maybe convert other kinds of node?
    }
  }
}
object Int32Type extends ScalarType("int32") {
  override val toString = "Int32Type"

  override type T = Int

  override def toJson(value: Int): JsonNode = IntNode.valueOf(value)
  override def fromJson(node: JsonNode): Either[String, Int] = {
    node match {
      case n: NumericNode if n.longValue() >= Int.MinValue && n.longValue() <= Int.MaxValue => Right(n.intValue())
      case _: NumericNode => failRange(node)
      case _ => fail(node) // TODO maybe convert other kinds of node?
    }
  }
}

object Int64Type extends ScalarType("int64") {
  override val toString = "Int64Type"

  override type T = Long

  override def toJson(value: Long): JsonNode = LongNode.valueOf(value)

  private val min = jm.BigInteger.valueOf(Long.MinValue)
  private val max = jm.BigInteger.valueOf(Long.MaxValue)
  override def fromJson(node: JsonNode): Either[String, Long] = {
    node match {
      case n: NumericNode if n.bigIntegerValue() >= min && n.bigIntegerValue() <= max => Right(n.longValue())
      case _: NumericNode => failRange(node)
      case _ => fail(node) // TODO maybe convert other kinds of node?
    }
  }
}

object UInt8Type extends ScalarType("uint8") {
  override val toString = "UInt8Type"

  override type T = Short

  override def toJson(value: Short): JsonNode = ShortNode.valueOf(value)

  private val max = 255
  override def fromJson(node: JsonNode): Either[String, Short] = {
    node match {
      case n: NumericNode if n.shortValue() >= 0 && n.shortValue() <= max => Right(n.shortValue())
      case _: NumericNode => failRange(node)
      case _ => Left(s"Can't convert $node to uint8") // TODO maybe convert other kinds of node?
    }
  }
}

object UInt16Type extends ScalarType("uint16") {
  override val toString = "UInt16Type"

  override type T = Int

  override def toJson(value: Int): JsonNode = IntNode.valueOf(value)

  private val max = 65536
  override def fromJson(node: JsonNode): Either[String, Int] = {
    node match {
      case n: NumericNode if n.intValue() >= 0 && n.intValue() <= max => Right(n.intValue())
      case _: NumericNode => failRange(node)
      case _ => Left(s"Can't convert $node to uint16") // TODO maybe convert other kinds of node?
    }
  }
}

object UInt32Type extends ScalarType("uint32") {
  override val toString = "UInt32Type"

  override type T = Long

  override def toJson(value: Long): JsonNode = LongNode.valueOf(value)

  private val max = 1L << 32
  override def fromJson(node: JsonNode): Either[String, Long] = {
    node match {
      case n: NumericNode if n.longValue() >= 0 && n.longValue() <= max => Right(n.longValue())
      case _: NumericNode => failRange(node)
      case _ => Left(s"Can't convert $node to uint32") // TODO maybe convert other kinds of node?
    }
  }
}

object UInt64Type extends ScalarType("uint64") {
  override type T = jm.BigInteger

  override def toJson(value: jm.BigInteger): JsonNode = BigIntegerNode.valueOf(value)

  private val max = BigInteger.valueOf(2L).pow(64)
  override def fromJson(node: JsonNode): Either[String, jm.BigInteger] = {
    node match {
      case n: NumericNode =>
        val big = n.bigIntegerValue()
        if (big < jm.BigInteger.ZERO || big > max) {
          failRange(node)
        } else {
          Right(big)
        }
      case _ => Left(s"Can't convert $node to uint64") // TODO maybe convert other kinds of node?
    }
  }
  override val toString = "UInt64Type"
}

object Float32Type extends ScalarType("float32") {
  override val toString = "Float32Type"

  override type T = Float

  override def toJson(value: Float): JsonNode = FloatNode.valueOf(value)
  override def fromJson(node: JsonNode): Either[String, Float] = {
    node match {
      case n: NumericNode => Right(n.floatValue()) // TODO check bounds?
      case _ => Left(s"Can't convert $node to float32") // TODO maybe convert other kinds of node?
    }
  }
}

object Float64Type extends ScalarType("float64") {
  override val toString = "Float64Type"

  override type T = Double

  override def toJson(value: Double): JsonNode = DoubleNode.valueOf(value)
  override def fromJson(node: JsonNode): Either[String, Double] = {
    node match {
      case n: NumericNode => Right(n.doubleValue()) // TODO check bounds?
      case _ => Left(s"Can't convert $node to float64") // TODO maybe convert other kinds of node?
    }
  }
}

case class DecimalType(precision: Int, scale: Int) extends ScalarType(s"decimal($precision,$scale)") {
  override type T = jm.BigDecimal
  override def toJson(value: jm.BigDecimal): JsonNode = DecimalNode.valueOf(value)
  override def fromJson(node: JsonNode): Either[String, java.math.BigDecimal] = {
    node match {
      case n: NumericNode =>
        Right(n.decimalValue()) // TODO is this right?
      case _ => Left(s"Can't convert $node to $decl") // TODO maybe convert other kinds of node?
    }
  }
}

case class TimestampType private (precision: Int) extends ScalarType(s"timestamp($precision)") {
  override type T = Instant
  require(precision >= 0, "Timestamp precision must be >= 0 (seconds) and <= 9 (nanoseconds)")
  require(precision <= 9, "Implementation limitation: maximum timestamp precision is 9 (nanoseconds)")

  override def toJson(value: Instant): JsonNode = {
    val truncated = precision match {
      case 0 => value.truncatedTo(ChronoUnit.SECONDS)
      case 3 => value.truncatedTo(ChronoUnit.MILLIS)
      case 6 => value.truncatedTo(ChronoUnit.MICROS)
      case _ => value
    }
    TextNode.valueOf(truncated.toString) // TODO maybe check that the trailing zeros are omitted
  }

  override def fromJson(node: JsonNode): Either[String, Instant] = {
    node match {
      case t: TextNode =>
        // TODO maybe truncate if incoming value has excessive precision?
        Right(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(t.textValue())))

      case n: NumericNode if n.canConvertToLong =>
        val l = n.longValue()
        precision match {
          case 0 => Right(Instant.ofEpochSecond(l))
          case 3 => Right(Instant.ofEpochMilli(l))
          case 6 => Right(connectors.microsToInstant(l))
          case 9 => Right(connectors.nanosToInstant(l))
          case other => Left(s"Unsupported timestamp precision $other")
        }
      case _ => fail(node)
    }
  }

  // All timestamp values are equivalent
  override val tryCast = {
    case (TimestampType(_), i: Instant) => Some(i)
    case _ => None
  }
}

object TimestampType {
  val Seconds = TimestampType(0)
  val Millis = TimestampType(3)
  val Micros = TimestampType(6)
  val Nanos = TimestampType(9)
}
