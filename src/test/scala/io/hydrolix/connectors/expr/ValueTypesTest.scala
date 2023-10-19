package io.hydrolix.connectors.expr

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connectors.types._

class ValueTypesTest {
  private val nestedStructType = StructType(StructField("nested.i", Int32Type), StructField("nested.s", StringType))

  private val structType = StructType(
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
  )

  private val structLiteral = StructLiteral(Map(
    "b" -> BooleanLiteral.True,
    "i8" -> Int8Literal(123),
    "u8" -> UInt8Literal(234),
    "i16" -> Int16Literal(31337),
    "u16" -> UInt16Literal(65535),
    "i32" -> Int32Literal(2 * 1024 * 1024),
    "u32" -> UInt32Literal(4 * 1024 * 1024),
    "i64" -> Int64Literal(2 * 1024 * 1024 * 1024),
    "u64" -> UInt64Literal(4 * 1024 * 1024 * 1024),
    "f32" -> Float32Literal(32.0f),
    "f64" -> Float64Literal(64.0f),
    "s" -> StringLiteral("hello world!"),
    "d" -> DecimalLiteral(java.math.BigDecimal.valueOf(3.14159265)),
    "array[i32!]" -> ArrayLiteral(List(1, 2, 3), Int32Type, false),
    "array[i32?]" -> ArrayLiteral(List(10, null, 20, null, 30), Int32Type, true),
    "map[string, f64!]" -> MapLiteral(Map("one" -> 1.0, "two" -> 2.0, "three" -> 3.0), StringType, Float64Type, false),
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
}
