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

import com.fasterxml.jackson.databind.JsonNode

trait ValueType extends Serializable {
  /** the JVM type of these values */
  type T

  def decl: String

  val tryCast: PartialFunction[(ValueType, Any), Option[T]]
}

/** For now this is just a marker to exclude AnyType */
trait ConcreteType extends ValueType {
  def toJson(value: T): JsonNode
  def fromJson(node: JsonNode): Either[String, T]
  def unsafeToJson(value: Any): JsonNode = toJson(value.asInstanceOf[T])
  def fail(node: JsonNode): Either[String, T] = Left(s"Can't convert $node to $decl")

  val tryCast: PartialFunction[(ValueType, Any), Option[T]] = {
    case (vt, value: Any) if vt == this => Some(value.asInstanceOf[T]) // Can always cast to same type
    case _ => None                                                     // Nothing else can cast by default
  }
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

  private def valueType[$: P]: P[ValueType] = P(any | concreteType)

  private def concreteType[$ : P]: P[ConcreteType] = P(scalar | map | array | struct)

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

  // TODO maybe allow map<concrete, any> sometimes?
  private def map[$: P] = P("map<" ~/ concreteType ~ cs ~ concreteType ~ cs ~ boolean ~ ">").map {
    case (kt, vt, nullable) => MapType(kt, vt, nullable)
  }

  // TODO maybe allow array<any> sometimes?
  private def array[$: P] = P("array<" ~/ concreteType ~ cs ~ boolean ~ ">").map {
    case (elt, nullable) => ArrayType(elt, nullable)
  }

  // TODO maybe allow struct fields of type <any> sometimes?
  private def struct[$: P] = P("struct<" ~/ (fieldname ~ ":" ~ concreteType).rep(min = 1, sep = cs) ~ ">").map { fields =>
    StructType(fields.map {
      case (name, typ) => StructField(name, typ)
    }.toList)
  }

  private def fieldname[$: P] = P("\"" ~ (CharPred(_ != '"') | escq).rep.! ~ "\"")
  private def escq[$ : P] = P("\\\"").map(_ => "\"")

  private def boolean[$: P] = P(IgnoreCase("true") | IgnoreCase("false")).!.map(_.toLowerCase.toBoolean)

  private def cs[$ : P] = P(" ".rep() ~ "," ~ " ".rep())
}

object AnyType extends ValueType {
  override type T = Any
  val decl = "<any>"

  // Anything can cast to Any!
  override val tryCast: PartialFunction[(ValueType, Any), Option[Any]] = {
    case (_, value) => Some(value)
  }
}
