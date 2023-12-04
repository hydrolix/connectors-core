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
import java.util.{Base64, Random}
import java.{math => jm}
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._

import io.hydrolix.connectors.types._

object DataGen {
  def apply(typ: ValueType, nullable: Boolean, rng: Random): JsonNode = {
    if (nullable && rng.nextDouble() < 0.1) {
      // TODO maybe make the null fraction customizable
      NullNode.instance
    } else {
      typ match {
        case BooleanType => BooleanNode.valueOf(rng.nextBoolean())
        case Int8Type => ShortNode.valueOf((rng.nextInt(256) - 127).toShort)
        case Int16Type => ShortNode.valueOf((rng.nextInt(65536) - 32767).toShort)
        case Int32Type => IntNode.valueOf(rng.nextInt())
        case Int64Type => LongNode.valueOf(rng.nextLong())
        case UInt8Type => ShortNode.valueOf(rng.nextInt(256).toShort)
        case UInt16Type => IntNode.valueOf(rng.nextInt(65536))
        case UInt32Type => LongNode.valueOf(rng.nextInt(Integer.MAX_VALUE))
        case Float64Type => DoubleNode.valueOf(rng.nextDouble())
        case TimestampType(0) => TextNode.valueOf(Instant.ofEpochSecond(rng.nextInt()).toString)
        case TimestampType(3) => TextNode.valueOf(Instant.ofEpochMilli(rng.nextLong()).toString)
        case TimestampType(6) => TextNode.valueOf(microsToInstant(rng.nextLong()).toString)

        case UInt64Type =>
          val l1 = rng.nextLong()
          val l2 = rng.nextLong()
          DecimalNode.valueOf(
            jm.BigDecimal.valueOf(l1).add(jm.BigDecimal.valueOf(l2))
          )
        case StringType =>
          val len = rng.nextInt(64) // empty is OK too
          val buf = Array.ofDim[Byte](len)
          rng.nextBytes(buf)
          TextNode.valueOf(Base64.getEncoder.encodeToString(buf))

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
}
