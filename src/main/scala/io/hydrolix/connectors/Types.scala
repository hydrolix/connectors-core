package io.hydrolix.connectors

import io.substrait.`type`.Type

object Types {
  private val arrayR = """array\((.*?)\)""".r
  private val mapR = """map\((.*?),\s*(.*?)\)""".r
  private val nullableR = """nullable\((.*?)\)""".r
  private val datetime64R = """datetime64\((.*?)\)""".r
  private val datetimeR = """datetime\((.*?)\)""".r
  private val encodingR = """(.*?)\s*encoding=(.*?)""".r

  private val PseudoUInt64 = Type.Decimal.builder.precision(20).scale(0).build()

  /**
   * TODO this is conservative about making space for rare, high-magnitude values, e.g. uint64 -> decimal ... should
   * probably be optional
   *
   * @return (sparkType, hdxColumnDataType, nullable?)
   */
  def decodeClickhouseType(s: String): (Type, HdxColumnDatatype, Boolean) = {
    s.toLowerCase match {
      case "int8" => (Type.I8, HdxColumnDatatype(HdxValueType.Int8, true, false), false) // signed 8-bit => byte
      case "uint8" => (Type.I16, HdxColumnDatatype(HdxValueType.UInt8, true, false), false) // unsigned 8-bit => short

      case "int16" => (Type.I16, HdxColumnDatatype(HdxValueType.Int32, true, false), false) // signed 16-bit => short
      case "uint16" => (Type.I32, HdxColumnDatatype(HdxValueType.UInt32, true, false), false) // unsigned 16-bit => int

      case "int32" => (Type.I32, HdxColumnDatatype(HdxValueType.Int32, true, false), false) // signed 32-bit => int
      case "uint32" => (Type.I64, HdxColumnDatatype(HdxValueType.UInt32, true, false), false) // unsigned 32-bit => long

      case "int64" => (Type.I64, HdxColumnDatatype(HdxValueType.Int64, true, false), false) // signed 64-bit => long
      case "uint64" => (PseudoUInt64, HdxColumnDatatype(HdxValueType.UInt64, true, false), false) // unsigned 64-bit => 20-digit decimal

      case "float32" => (Type.FP32, HdxColumnDatatype(HdxValueType.Double, true, false), false) // float32 => double
      case "float64" => (Type.FP64, HdxColumnDatatype(HdxValueType.Double, true, false), false) // float64 => double

      case "string" => (Type.Str, HdxColumnDatatype(HdxValueType.String, true, false), false)

      case datetime64R(_) => (Type.Timestamp, HdxColumnDatatype(HdxValueType.DateTime64, true, false), false) // TODO OK to discard precision here?
      case datetimeR(_) => (Type.Timestamp, HdxColumnDatatype(HdxValueType.DateTime, true, false), false) // TODO OK to discard precision here?

      case "datetime64" => (Type.Timestamp, HdxColumnDatatype(HdxValueType.DateTime64, true, false), false)
      case "datetime" => (Type.Timestamp, HdxColumnDatatype(HdxValueType.DateTime64, true, false), false)

      case arrayR(elementTypeName) =>
        val (typ, hdxType, nullable) = decodeClickhouseType(elementTypeName)

        (
          Type.ListType.builder()
          .elementType(typ)
          .build(),
          HdxColumnDatatype(HdxValueType.Array, true, false, elements = Some(List(hdxType))),
          nullable
        )

      case mapR(keyTypeName, valueTypeName) =>
        val (keyType, hdxKeyType, _) = decodeClickhouseType(keyTypeName)
        val (valueType, hdxValueType, valueNull) = decodeClickhouseType(valueTypeName)

        (
          Type.Map.builder()
            .key(keyType)
            .value(valueType)
            .nullable(valueNull)
            .build(),
          HdxColumnDatatype(HdxValueType.Map, true, false, elements = Some(List(hdxKeyType, hdxValueType))),
          false
        )

      case nullableR(typeName) =>
        val (typ, hdxType, _) = decodeClickhouseType(typeName)

        (typ, hdxType, true)

      case encodingR(name, _) =>
        // TODO we might want the encoding somewhere but not for Spark per se
        decodeClickhouseType(name)
    }
  }

  def hdxToSubstrait(htype: HdxColumnDatatype): Type = {
    htype.`type` match {
      case HdxValueType.Int8 => Type.I8
      case HdxValueType.UInt8 => Type.I16
      case HdxValueType.Int32 => Type.I32
      case HdxValueType.UInt32 => Type.I64
      case HdxValueType.Int64 => Type.I64
      case HdxValueType.UInt64 => PseudoUInt64
      case HdxValueType.Double => Type.FP64
      case HdxValueType.String => Type.Str
      case HdxValueType.Boolean => Type.Bool
      case HdxValueType.DateTime64 => Type.Timestamp
      case HdxValueType.DateTime => Type.Timestamp
      case HdxValueType.Epoch => Type.Timestamp
      case HdxValueType.Array =>
        val elt = hdxToSubstrait(htype.elements.get.head)
        Type.ListType.builder()
          .elementType(elt)
          .build()
      case HdxValueType.Map =>
        val kt = hdxToSubstrait(htype.elements.get.apply(0))
        val vt = hdxToSubstrait(htype.elements.get.apply(1))
        Type.Map.builder()
          .key(kt)
          .value(vt)
          .build()
    }
  }

}