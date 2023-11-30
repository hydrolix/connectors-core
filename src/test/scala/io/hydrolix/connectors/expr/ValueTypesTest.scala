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

import com.fasterxml.jackson.databind.node.ObjectNode
import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connectors.JSON
import io.hydrolix.connectors.partitionreader.CoreRowAdapter
import io.hydrolix.connectors.types._

class ValueTypesTest {
  private val nestedStructType = StructType(List(StructField("nested.i", Int32Type), StructField("nested.s", StringType)))

  private val structType = StructType(List(
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
    StructField("array[i32!]", ArrayType(Int32Type, false)),
    StructField("array[i32?]", ArrayType(Int32Type, true)),
    StructField("map[string, f64!]", MapType(StringType, Float64Type, false)),
    StructField("nested", nestedStructType)
  ))

  private val structLiteral = StructLiteral(Map(
    "b" -> true,
    "i8" -> 123.toByte,
    "u8" -> 234.toShort,
    "i16" -> 31337.toShort,
    "u16" -> 65535.toInt,
    "i32" -> 2 * 1024 * 1024,
    "u32" -> (4L * 1024 * 1024),
    "i64" -> (2L * 1024 * 1024 * 1024),
    "u64" -> java.math.BigDecimal.valueOf(4L * 1024 * 1024 * 1024),
    "f32" -> 32.0f,
    "f64" -> 64.0d,
    "s" -> "hello world!",
    "d" -> java.math.BigDecimal.valueOf(3.14159265),
    "array[i32!]" -> List(1, 2, 3),
    "array[i32?]" -> List(10, null, 20, null, 30),
    "map[string, f64!]" -> Map("one" -> 1.0, "two" -> 2.0, "three" -> 3.0),
    "nested" -> StructLiteral(Map("nested.i" -> 123, "nested.s" -> "yolo"), nestedStructType)
  ), structType)

  private val structTypeName =
    """struct<
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
      |"map[string, f64!]":map<string,float64,false>,
      |"nested":struct<"nested.i":int32,"nested.s":string>
      |>""".stripMargin.replace("\n", "")

  private val rowJson =
    """{
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
      |  "map[string, f64!]":{
      |    "one":1.0,
      |    "two":2.0,
      |    "three":3.0
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
    assertEquals(structType.fields.size, structLiteral.values.size)
    assertEquals(structLiteral.value.size, structLiteral.values.size)

    for ((sf, i) <- structType.fields.zipWithIndex) {
      val valueByName = structLiteral.value(sf.name)
      val valueByPos = structLiteral.values(i)

      assertEquals(valueByName, valueByPos)
    }
  }

  @Test
  def `ser/des all the things`(): Unit = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(structLiteral)
    oos.close()
    val bytes = baos.toByteArray
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val structLiteral2 = ois.readObject().asInstanceOf[StructLiteral]
    println(structLiteral2)
    assertEquals(structLiteral, structLiteral2)
  }

  @Test
  def `parse records to structs`(): Unit = {
    val obj = JSON.objectMapper.readTree(rowJson).asInstanceOf[ObjectNode]
    val row = CoreRowAdapter.row(-1, structType, obj)
    println(row)
    assertEquals(structLiteral, row)
  }
}
