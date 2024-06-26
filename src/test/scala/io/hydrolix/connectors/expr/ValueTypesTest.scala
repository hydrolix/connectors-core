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

package io.hydrolix.connectors.expr

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.math.BigInteger
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.{math => jm}

import com.fasterxml.jackson.databind.node.{BigIntegerNode, ObjectNode}
import org.junit.Assert.assertEquals
import org.junit.{Assert, Test}

import io.hydrolix.connectors.JSON
import io.hydrolix.connectors.data.{CoreRowAdapter, Row}
import io.hydrolix.connectors.types._

class ValueTypesTest {
  private val nestedStructType = StructType(List(StructField("nested.i", Int32Type), StructField("nested.s", StringType)))

  private val arrayOfIntNoNulls: ArrayType = ArrayType(Int32Type, false)
  private val arrayOfIntNullable: ArrayType = ArrayType(Int32Type, true)
  private val arrayOfStringNullable: ArrayType = ArrayType(StringType, true)

  private val structType = StructType(List(
    StructField("timestamp0", TimestampType.Seconds),
    StructField("timestamp3", TimestampType.Millis),
    StructField("timestamp6", TimestampType.Micros),
    StructField("timestamp0n", TimestampType.Seconds),
    StructField("timestamp3n", TimestampType.Millis),
    StructField("timestamp6n", TimestampType.Micros),
    StructField("b", BooleanType),
    StructField("i8", Int8Type),
    StructField("u8", UInt8Type),
    StructField("i16", Int16Type),
    StructField("u16", UInt16Type),
    StructField("i32", Int32Type),
    StructField("u32", UInt32Type),
    StructField("i64", Int64Type),
    StructField("u64", UInt64Type),
    StructField("f32", Float32Type),
    StructField("f64", Float64Type),
    StructField("s", StringType),
    StructField("d", DecimalType(20, 3)),
    StructField("array[i32!]", arrayOfIntNoNulls),
    StructField("array[i32?]", arrayOfIntNullable),
    StructField("array[array[string?]!]", ArrayType(arrayOfStringNullable, false)),
    StructField("map[string, f64!]", MapType(StringType, Float64Type, false)),
    StructField("map[string, i8?]", MapType(StringType, Int8Type, true)),
    StructField("nested", nestedStructType)
  ))

  private val structRow = structType.mapToRow(Map(
    "timestamp0" -> Instant.EPOCH,
    "timestamp3" -> Instant.EPOCH.plusMillis(123),
    "timestamp6" -> Instant.EPOCH.plus(123456, ChronoUnit.MICROS),
    "timestamp0n" -> Instant.EPOCH,
    "timestamp3n" -> Instant.EPOCH.plusMillis(123),
    "timestamp6n" -> Instant.EPOCH.plus(123456, ChronoUnit.MICROS),
    "b" -> true,
    "i8" -> 123.toByte,
    "u8" -> 234.toShort,
    "i16" -> 31337.toShort,
    "u16" -> 65535,
    "i32" -> 2 * 1024 * 1024,
    "u32" -> (4L * 1024 * 1024),
    "i64" -> (2L * 1024 * 1024 * 1024),
    "u64" -> jm.BigInteger.valueOf(4L * 1024 * 1024 * 1024),
    "f32" -> 32.0f,
    "f64" -> 64.0d,
    "s" -> "hello world!",
    "d" -> jm.BigDecimal.valueOf(3.14159265),
    "array[i32!]" -> List(1, 2, 3),
    "array[i32?]" -> List(10, null, 20, null, 30),
    "array[array[string?]!]" -> List(
      List("one", "two", null, "four")
    ),
    "map[string, f64!]" -> Map("one" -> 1.0, "two" -> 2.0, "three" -> 3.0),
    "map[string, i8?]" -> Map("1b" -> 1.toByte, "2b" -> null, "3b" -> 3.toByte, "4b" -> null),
    "nested" -> nestedStructType.mapToRow(Map("nested.i" -> 123, "nested.s" -> "yolo"))
  ))

  private val structTypeName =
    """struct<
      |"timestamp0":timestamp(0),
      |"timestamp3":timestamp(3),
      |"timestamp6":timestamp(6),
      |"timestamp0n":timestamp(0),
      |"timestamp3n":timestamp(3),
      |"timestamp6n":timestamp(6),
      |"b":boolean,
      |"i8":int8,
      |"u8":uint8,
      |"i16":int16,
      |"u16":uint16,
      |"i32":int32,
      |"u32":uint32,
      |"i64":int64,
      |"u64":uint64,
      |"f32":float32,
      |"f64":float64,
      |"s":string,
      |"d":decimal(20,3),
      |"array[i32!]":array<int32,false>,
      |"array[i32?]":array<int32,true>,
      |"array[array[string?]!]":array<array<string,true>,false>,
      |"map[string, f64!]":map<string,float64,false>,
      |"map[string, i8?]":map<string,int8,true>,
      |"nested":struct<"nested.i":int32,"nested.s":string>
      |>""".stripMargin.replace("\n", "")

  private val rowJson =
    """{
      |  "timestamp0":"1970-01-01T00:00:00Z",
      |  "timestamp3":"1970-01-01T00:00:00.123Z",
      |  "timestamp6":"1970-01-01T00:00:00.123456Z",
      |  "timestamp0n":0,
      |  "timestamp3n":123,
      |  "timestamp6n":123456,
      |  "b":true,
      |  "i8":123,
      |  "u8":234,
      |  "i16":31337,
      |  "u16":65535,
      |  "i32":2097152,
      |  "u32":4194304,
      |  "i64":2147483648,
      |  "u64":4294967296,
      |  "f32":32.0,
      |  "f64":64.0,
      |  "s":"hello world!",
      |  "d":3.14159265,
      |  "array[i32!]":[1,2,3],
      |  "array[i32?]":[10,null,20,null,30],
      |  "array[array[string?]!]":[["one","two",null,"four"]],
      |  "map[string, f64!]":{
      |    "one":1.0,
      |    "two":2.0,
      |    "three":3.0
      |  },
      |  "map[string, i8?]":{
      |    "1b":1,
      |    "2b":null,
      |    "3b":3,
      |    "4b":null
      |  },
      |  "nested":{
      |    "nested.i":123,
      |    "nested.s":"yolo"
      |   }
      |}""".stripMargin

  @Test
  def `ValueType decls render OK`(): Unit = {
    assertEquals(structTypeName, structType.decl)
  }

  @Test
  def `ValueType decls parse OK`(): Unit = {
    assertEquals(structType, ValueType.parse(structTypeName))
  }

  @Test
  def `ValueType.parse works`(): Unit = {
    println(ValueType.parse("""struct<"timestamp":timestamp(3), "level":string, "message":string>"""))
  }

  @Test
  def `struct with every type of literal`(): Unit = {
    assertEquals(structType.fields.size, structRow.values.size)
    val map = structType.rowToMap(structRow)
    assertEquals(structRow.values.size, map.size)

    for ((sf, i) <- structType.fields.zipWithIndex) {
      val valueByName = map(sf.name)
      val valueByPos = structRow.values(i)

      assertEquals(valueByName, valueByPos)
    }
  }

  @Test
  def `ser/des all the things`(): Unit = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(structRow)
    oos.close()
    val bytes = baos.toByteArray
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val structLiteral2 = ois.readObject().asInstanceOf[Row]
    println(structLiteral2)
    assertEquals(structRow, structLiteral2)
  }

  @Test
  def `parse records to structs`(): Unit = {
    val obj = JSON.objectMapper.readTree(rowJson).asInstanceOf[ObjectNode]
    val row = CoreRowAdapter.row(-1, structType, obj)
    println(row)
    assertEquals(structRow, row)
  }

  @Test
  def `toJson/fromJson round trip`(): Unit = {
    val rowJson = structType.toJson(structRow)
    val parsed = structType.fromJson(rowJson)
    assertEquals(Right(structRow), parsed)
  }

  @Test
  def `try out-of-range values`(): Unit = {
    val badValues = Map(
      Int8Type -> List(-129, 128), // signed byte
      Int16Type -> List(-32769, 32768), // signed short
      Int32Type -> List(Int.MinValue - 1L, Int.MaxValue + 1L), // signed int32
      Int64Type -> List(
        jm.BigDecimal.valueOf(Long.MinValue).subtract(jm.BigDecimal.ONE),
        jm.BigDecimal.valueOf(Long.MaxValue).add(jm.BigDecimal.ONE)
      ), // signed int64
      UInt8Type -> List(-1, 256), // unsigned u8, stored in a short
      UInt16Type -> List(-1, 65537), // unsigned u16, stored in an int
      UInt32Type -> List(-1, (1L << 32) + 1), // unsigned u32, stored in a long
      UInt64Type -> List(-1, new jm.BigDecimal(2L).pow(64).add(jm.BigDecimal.ONE)), // unsigned u64, stored in a BigDecimal
    )

    for ((typ, values) <- badValues) {
      for (value <- values) {
        val num = value.asInstanceOf[Number]
        val node = BigIntegerNode.valueOf(new BigInteger(num.toString))
        typ.fromJson(node) match {
          case Right(ok) => Assert.fail(s"Expected conversion failure of ${typ.decl} = $value, got $ok")
          case Left(_) => () // expected failure
        }
      }
    }
  }
}
