package io.hydrolix.connectors

object Types {
  private val arrayR = """array\((.*?)\)""".r
  private val mapR = """map\((.*?),\s*(.*?)\)""".r
  private val nullableR = """nullable\((.*?)\)""".r
  private val datetime64R = """datetime64\((.*?)\)""".r
  private val datetimeR = """datetime\((.*?)\)""".r
  private val encodingR = """(.*?)\s*encoding=(.*?)""".r

  /**
   * TODO this is conservative about making space for rare, high-magnitude values, e.g. uint64 -> decimal ... should
   * probably be optional
   *
   * @return (sparkType, hdxColumnDataType, nullable?)
   */
  def decodeClickhouseType(s: String): (ValueType[_], HdxColumnDatatype, Boolean) = {
    s.toLowerCase match {
      case "int8" => (Int8Type, HdxColumnDatatype(HdxValueType.Int8, true, false), false) // signed 8-bit => byte
      case "uint8" => (UInt8Type, HdxColumnDatatype(HdxValueType.UInt8, true, false), false) // unsigned 8-bit => short

      case "int16" => (Int16Type, HdxColumnDatatype(HdxValueType.Int32, true, false), false) // signed 16-bit => short
      case "uint16" => (UInt16Type, HdxColumnDatatype(HdxValueType.UInt32, true, false), false) // unsigned 16-bit => int

      case "int32" => (Int32Type, HdxColumnDatatype(HdxValueType.Int32, true, false), false) // signed 32-bit => int
      case "uint32" => (UInt32Type, HdxColumnDatatype(HdxValueType.UInt32, true, false), false) // unsigned 32-bit => long

      case "int64" => (Int64Type, HdxColumnDatatype(HdxValueType.Int64, true, false), false) // signed 64-bit => long
      case "uint64" => (UInt64Type, HdxColumnDatatype(HdxValueType.UInt64, true, false), false) // unsigned 64-bit => 20-digit decimal

      case "float32" => (Float32Type, HdxColumnDatatype(HdxValueType.Double, true, false), false) // float32 => double
      case "float64" => (Float64Type, HdxColumnDatatype(HdxValueType.Double, true, false), false) // float64 => double

      case "string" => (StringType, HdxColumnDatatype(HdxValueType.String, true, false), false)

      case datetime64R(_) => (TimestampType.Millis, HdxColumnDatatype(HdxValueType.DateTime64, true, false), false) // TODO OK to discard precision here?
      case datetimeR(_) => (TimestampType.Seconds, HdxColumnDatatype(HdxValueType.DateTime, true, false), false) // TODO OK to discard precision here?

      case "datetime64" => (TimestampType.Millis, HdxColumnDatatype(HdxValueType.DateTime64, true, false), false)
      case "datetime" => (TimestampType.Seconds, HdxColumnDatatype(HdxValueType.DateTime64, true, false), false)

      case arrayR(elementTypeName) =>
        val (typ, hdxType, nullable) = decodeClickhouseType(elementTypeName)

        (ArrayType(typ, nullable), hdxType, false)

      case mapR(keyTypeName, valueTypeName) =>
        val (keyType, hdxKeyType, _) = decodeClickhouseType(keyTypeName)
        val (valueType, hdxValueType, valueNull) = decodeClickhouseType(valueTypeName)

        (MapType(keyType, valueType, valueNull), HdxColumnDatatype(HdxValueType.Map, true, false, elements = Some(List(hdxKeyType, hdxValueType))), false)

      case nullableR(typeName) =>
        val (typ, hdxType, _) = decodeClickhouseType(typeName)

        (typ, hdxType, true)

      case encodingR(name, _) =>
        // TODO we might want the encoding somewhere but not for Spark per se
        decodeClickhouseType(name)
    }
  }

  def hdxToValueType(htype: HdxColumnDatatype): ValueType[_] = {
    htype.`type` match {
      case HdxValueType.Int8 => Int8Type
      case HdxValueType.UInt8 => UInt8Type
      case HdxValueType.Int32 => Int32Type
      case HdxValueType.UInt32 => UInt32Type
      case HdxValueType.Int64 => Int64Type
      case HdxValueType.UInt64 => UInt64Type
      case HdxValueType.Double => Float64Type
      case HdxValueType.String => StringType
      case HdxValueType.Boolean => BooleanType
      case HdxValueType.DateTime64 => TimestampType.Millis
      case HdxValueType.DateTime => TimestampType.Seconds
      case HdxValueType.Epoch => TimestampType.Millis // TODO is this right?
      case HdxValueType.Array =>
        val elt = hdxToValueType(htype.elements.get.head)
        ArrayType(elt, false)
      case HdxValueType.Map =>
        val kt = hdxToValueType(htype.elements.get.apply(0))
        val vt = hdxToValueType(htype.elements.get.apply(1))
        MapType(kt, vt, false)
    }
  }

}