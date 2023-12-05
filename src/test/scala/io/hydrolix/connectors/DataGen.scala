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

package io.hydrolix.connectors

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Base64, Random}
import java.{math => jm}
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._

import io.hydrolix.connectors.api.HdxColumnDatatype
import io.hydrolix.connectors.types._

object DataGen {
  val minDateTime = Instant.EPOCH
  val maxDateTime = Instant.parse("2106-02-07T06:28:15.000Z")
  val minDateTime64 = Instant.parse("1900-01-01T00:00:00.000Z")
  val maxDateTime64 = Instant.parse("2299-12-31T00:00:00.000Z")

  def apply(hcol: HdxColumnDatatype, nullable: Boolean, rng: Random): JsonNode = {
    if (nullable && rng.nextDouble() < 0.1) {
      // TODO maybe make the null fraction customizable
      NullNode.instance
    } else {
      hcol.`type` match {
        case HdxValueType.Boolean => bool(rng)
        case HdxValueType.Int8 => int8(rng)
        case HdxValueType.Int32 => int32(rng)
        case HdxValueType.Int64 => int64(rng)
        case HdxValueType.UInt8 => uint8(rng)
        case HdxValueType.UInt32 => uint32(rng)
        case HdxValueType.UInt64 => uint64(rng)
        case HdxValueType.Double => float64(rng)
        case HdxValueType.String => string(rng) // empty is OK too
        case HdxValueType.DateTime =>
          val when = randomTime(hcol, rng)

          // TODO replicate the golang format?
          TextNode.valueOf(when.toString)

        case HdxValueType.Epoch =>
          val when = randomTime(hcol, rng)

          hcol.format match {
            case Some("s") | Some("second") => LongNode.valueOf(when.getEpochSecond)
            case Some("ms") => LongNode.valueOf(when.toEpochMilli)
            case Some("us") => LongNode.valueOf(instantToMicros(when))
          }

        case HdxValueType.Array =>
          val len = rng.nextInt(8)
          val elt = hcol.elements.get.head
          val values = List.fill(len) {
            DataGen(
              elt,
              elt.`type` != HdxValueType.Array, // nested arrays are secretly not nullable
              rng
            )
          }

          JSON.objectMapper.createArrayNode().also { arr =>
            arr.addAll(values.asJava)
          }

        case HdxValueType.Map =>
          val len = rng.nextInt(8)
          val keys = List.fill(len)(DataGen(StringType, false, rng)).map(_.asInstanceOf[TextNode].asText())
          val values = List.fill(len)(DataGen(hcol.elements.get(1), true, rng))

          JSON.objectMapper.createObjectNode().also { obj =>
            for ((k, v) <- keys.zip(values)) {
              obj.replace(k, v)
            }
          }
      }
    }
  }

  private def randomTime(hcol: HdxColumnDatatype, rng: Random) = {
    val when = if (hcol.primary) {
      // Primary has to be recent
      Instant.now()
    } else hcol.resolution match {
      case Some("s") | Some("second") | Some("seconds") =>
        val when = rng.longs(1L, minDateTime.getEpochSecond, maxDateTime.getEpochSecond).max.getAsLong
        Instant.ofEpochSecond(when)
      case Some("ms") =>
        val when = rng.longs(1L, minDateTime64.toEpochMilli, maxDateTime64.toEpochMilli).max.getAsLong
        Instant.ofEpochMilli(when)
    }

    when.truncatedTo(ChronoUnit.MILLIS)
  }

  def apply(typ: ValueType, nullable: Boolean, rng: Random): JsonNode = {
    if (nullable && rng.nextDouble() < 0.1) {
      // TODO maybe make the null fraction customizable
      NullNode.instance
    } else {
      typ match {
        case BooleanType => bool(rng)
        case Int8Type => int8(rng)
        case Int16Type => ShortNode.valueOf((rng.nextInt(65536) - 32767).toShort)
        case Int32Type => int32(rng)
        case Int64Type => int64(rng)
        case UInt8Type => uint8(rng)
        case UInt16Type => IntNode.valueOf(rng.nextInt(65536))
        case UInt32Type => uint32(rng)
        case UInt64Type => uint64(rng)
        case Float64Type => float64(rng)
        case TimestampType(0) => TextNode.valueOf(Instant.ofEpochSecond(rng.nextInt()).toString)
        case TimestampType(3) => TextNode.valueOf(Instant.ofEpochMilli(rng.nextLong()).toString)
        case TimestampType(6) => TextNode.valueOf(microsToInstant(rng.nextLong()).toString)

        case StringType =>
          string(rng)

        case DecimalType(precision, scale) =>
          val sign = if (rng.nextBoolean()) "" else "-"
          val len = rng.nextInt(precision + 1)
          val digits = Array.ofDim[Byte](len)
          rng.nextBytes(digits)
          val correctedDigits = digits.map(_.abs % 10).mkString("").dropWhile(_ == '0')
          val (pre, suf) = correctedDigits.splitAt(correctedDigits.length - scale)
          DecimalNode.valueOf(new java.math.BigDecimal(s"$sign$pre.$suf"))

        case ArrayType(elt, nullable) =>
          val len = rng.nextInt(8)
          val values = List.fill(len)(DataGen(elt, nullable, rng))
          JSON.objectMapper.createArrayNode().also { arr =>
            arr.addAll(values.asJava)
          }

        case MapType(_, vt, nullable) =>
          val len = rng.nextInt(8)
          val keys = List.fill(len)(DataGen(StringType, false, rng)).map(_.asInstanceOf[TextNode].asText())
          val values = List.fill(len)(DataGen(vt, nullable, rng))

          JSON.objectMapper.createObjectNode().also { obj =>
            for ((k, v) <- keys.zip(values)) {
              obj.replace(k, v)
            }
          }

        case StructType(fields) =>
          val values = fields.map { sf =>
            sf.name -> DataGen(sf.`type`, sf.nullable, rng)
          }

          JSON.objectMapper.createObjectNode().also { obj =>
            for ((k, v) <- values) {
              obj.replace(k, v)
            }
          }
      }
    }
  }

  private def float64(rng: Random) = {
    DoubleNode.valueOf(rng.nextDouble())
  }

  private def uint64(rng: Random) = {
    val longs = rng.longs(2L, 0, Long.MaxValue).toArray

    BigIntegerNode.valueOf(
      jm.BigInteger.valueOf(longs(0)).add(jm.BigInteger.valueOf(longs(1)))
    )
  }

  private def uint8(rng: Random) = {
    ShortNode.valueOf(rng.nextInt(256).toShort)
  }

  private def int64(rng: Random) = {
    LongNode.valueOf(rng.nextLong())
  }

  private def int32(rng: Random) = {
    IntNode.valueOf(rng.nextInt())
  }

  private def int8(rng: Random) = {
    ShortNode.valueOf((rng.nextInt(256) - 127).toShort)
  }

  private def bool(rng: Random) = {
    BooleanNode.valueOf(rng.nextBoolean())
  }

  private def string(rng: Random) = {
    val len = rng.nextInt(64) // empty is OK too
    val buf = Array.ofDim[Byte](len)
    rng.nextBytes(buf)
    TextNode.valueOf(Base64.getEncoder.encodeToString(buf))
  }

  private def uint32(rng: Random) = {
    LongNode.valueOf(rng.nextInt(Integer.MAX_VALUE))
  }
}
