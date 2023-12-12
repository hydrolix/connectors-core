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
package types

import java.{util => ju}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode, ValueNode}

import io.hydrolix.connectors.expr.Row
import io.hydrolix.connectors.{Etc, JSON}

sealed trait ComplexType extends ConcreteType

case class ArrayType(elementType: ConcreteType, elementsNullable: Boolean = false) extends ComplexType {
  override type T = Seq[elementType.T]
  override def decl: String = s"array<${elementType.decl},$elementsNullable>"

  override def toJson(value: Seq[elementType.T]): JsonNode = {
    JSON.objectMapper.createArrayNode().also { arr =>
      for ((v, i) <- value.zipWithIndex) {
        if (v == null) {
          if (elementsNullable) {
            arr.addNull()
          } else {
            sys.error(s"Null element #$i of a non-nullable array")
          }
        } else arr.add(elementType.toJson(v))
      }
    }
  }

  override def fromJson(node: JsonNode): Either[String, Seq[elementType.T]] = {
    node match {
      case arr: ArrayNode =>
        val values = new ju.ArrayList[AnyRef]()
        val nulls = mutable.BitSet()
        val errors = ListBuffer[String]()

        for ((node, i) <- arr.asScala.zipWithIndex) {
          if (node == null || node.isNull) {
            if (elementsNullable) {
              nulls(i) = true
            } else {
              errors += s"Null element #$i of a non-nullable array"
            }
          } else {
            elementType.fromJson(node) match {
              case Right(v) =>
                values.ensureCapacity(i+1) // this seems redundant with the next line, but it avoids repeated re-allocations
                while (values.size() < i+1) values.add(null)
                values.set(i, v.asInstanceOf[AnyRef])
              case Left(err) => errors += err
            }
          }
        }

        if (errors.nonEmpty) {
          Left(s"At least one element in array $node couldn't be converted: $errors")
        } else {
          Right(values.asInstanceOf[ju.ArrayList[elementType.T]].asScala.toSeq)
        }

      case v: ValueNode if !v.isNull =>
        elementType.fromJson(node).map(v => Vector(v))

      case _ => fail(node)
    }
  }
}

case class MapType(keyType: ConcreteType, valueType: ConcreteType, valuesNullable: Boolean = false) extends ComplexType {
  override type T = Map[keyType.T, valueType.T]
  override def decl: String = s"map<${keyType.decl},${valueType.decl},$valuesNullable>"

  override def toJson(value: Map[keyType.T, valueType.T]): JsonNode = {
    JSON.objectMapper.createObjectNode().also { obj =>
      for ((k, v) <- value) {
        val sk = keyType.toJson(k).textValue()
        if (sk == null) sys.error("TODO maps with non-String keys")

        if (v == null) {
          if (!valuesNullable) {
            sys.error(s"Null value for key $k in non-nullable map")
          } else {
            obj.putNull(sk)
          }
        } else {
          obj.replace(sk, valueType.toJson(v))
        }
      }
    }
  }

  override def fromJson(node: JsonNode): Either[String, Map[keyType.T, valueType.T]] = {
    node match {
      case obj: ObjectNode =>
        val (badKeys, goodKeys) = obj.fieldNames().asScala.toSeq.map(s => keyType.fromJson(TextNode.valueOf(s))).splitEithers
        val (badValues, goodValues) = obj.elements().asScala.toSeq.map { v =>
          if (v.isNull) {
            if (valuesNullable) {
              Right(null.asInstanceOf[valueType.T])
            } else {
              Left(s"Null value in non-nullable map") // TODO put $k in here too
            }
          } else {
            valueType.fromJson(v)
          }
        }.splitEithers

        if (badKeys.nonEmpty || badValues.nonEmpty) {
          Left(s"At least one key or value couldn't be converted: ${badKeys ++ badValues}")
        } else {
          Right(goodKeys.zip(goodValues).toMap)
        }
      case _ => fail(node)
    }
  }
}

case class StructField(name: String, `type`: ConcreteType, nullable: Boolean = false)

case class StructType(fields: List[StructField]) extends ComplexType {
  @transient lazy val byName: Map[String, StructField] = fields.map(sf => sf.name -> sf).toMap

  override type T = Row

  override def toJson(value: Row): JsonNode = {
    JSON.objectMapper.createObjectNode().also { obj =>
      for ((field, value) <- fields.zip(value.values)) {
        val node = field.`type`.unsafeToJson(value)
        obj.replace(field.name, node)
      }
    }
  }

  override def fromJson(node: JsonNode): Either[String, Row] = {
    node match {
      case obj: ObjectNode =>
        val (bads, goods) = obj.fieldNames().asScala.toSeq.map { name =>
          val field = byName(name)
          field.`type`.fromJson(obj.get(name)) match {
            case Right(r) => Right(name -> r.asInstanceOf[AnyRef])
            case Left(l) => Left(name -> l)
          }
        }.splitEithers

        if (bads.nonEmpty) {
          Left(s"At least one field couldn't be converted: ${bads.toMap}")
        } else {
          Right(mapToRow(goods.toMap))
        }
      case _ => fail(node)
    }
  }

  def mapToRow(values: Map[String, Any]): Row = {
    Row(fields.map { f =>
      values.getOrElse(f.name, null)
    })
  }

  def rowToMap(row: Row): Map[String, Any] = {
    val out = new ju.HashMap[String, Any]()
    for ((f, i) <- fields.zipWithIndex) {
      val value = row.values(i)
      out.put(f.name, value)
    }
    out.asScala.toMap
  }

  override def decl: String = {
    val sb = new StringBuilder()
    sb.append("struct<")
    val fdecls = fields.map { sf =>
      s""""${sf.name}":${sf.`type`.decl}"""
    }
    sb.append(fdecls.mkString(","))
    sb.append(">")
    sb.toString()
  }
}
