package io.hydrolix.connectors.types

trait ValueType extends Serializable {
  def decl: String
}

object ValueType {
  import fastparse._
  import NoWhitespace._

  def parse(s: String): ValueType = {
    fastparse.parse(s, valueType(_)) match {
      case Parsed.Success(vt, _) => vt
      case pf: Parsed.Failure => sys.error(s"Couldn't parse ValueType from $s: $pf")
    }
  }

  private def valueType[$: P]: P[ValueType] = P(any | scalar | map | array | struct)

  private def any[$: P] = P("<any>").map(_ => AnyType)

  private def decimal[$ : P] = P("decimal(" ~/ CharsWhile(_.isDigit).! ~ cs ~ CharsWhile(_.isDigit).! ~ ")").map {
    case (p, s) => DecimalType(p.toInt, s.toInt)
  }

  private def timestamp[$: P] = P("timestamp(" ~/ CharsWhile(_.isDigit).! ~ ")").map { precision =>
    TimestampType(precision.toInt)
  }

  private def scalar[$: P] = P(
    decimal | timestamp |
      "int8" | "int16" | "int32" | "int64" |
      "uint8" | "uint16" | "uint32" | "uint64" |
      "float32" | "float64" |
      "string" | "boolean").!.flatMap { name =>
    ScalarType.byName.get(name)
      .map(Pass(_))
      .getOrElse(Fail(s"Unknown scalar type: $name"))
  }

  private def map[$: P] = P("map<" ~/ valueType ~ cs ~ valueType ~ cs ~ boolean ~ ">").map {
    case (kt, vt, nullable) => MapType(kt, vt, nullable)
  }

  private def array[$: P] = P("array<" ~/ valueType ~ cs ~ boolean ~ ">").map {
    case (elt, nullable) => ArrayType(elt, nullable)
  }

  private def struct[$: P] = P("struct<" ~/ (fieldname ~ ":" ~ valueType).rep(min = 1, sep = cs) ~ ">").map { fields =>
    StructType(fields.map {
      case (name, typ) => StructField(name, typ)
    }: _*)
  }

  private def fieldname[$: P] = P("\"" ~ (CharPred(_ != '"') | escq).rep.! ~ "\"")
  private def escq[$ : P] = P("\\\"").map(_ => "\"")

  private def boolean[$: P] = P(IgnoreCase("true") | IgnoreCase("false")).!.map(_.toLowerCase.toBoolean)

  private def cs[$ : P] = P(" ".rep() ~ "," ~ " ".rep())
}

object AnyType extends ValueType {
  val decl = "<any>"
}
