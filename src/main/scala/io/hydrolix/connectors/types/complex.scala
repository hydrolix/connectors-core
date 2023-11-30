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

sealed trait ComplexType extends ValueType

case class ArrayType(elementType: ValueType, elementsNullable: Boolean = false) extends ComplexType {
  override def decl: String = s"array<${elementType.decl},$elementsNullable>"
}

case class MapType(keyType: ValueType, valueType: ValueType, valuesNullable: Boolean = false) extends ComplexType {
  override def decl: String = s"map<${keyType.decl},${valueType.decl},$valuesNullable>"
}

case class StructField(name: String, `type`: ValueType, nullable: Boolean = false)

case class StructType(fields: List[StructField]) extends ComplexType {
  @transient lazy val byName: Map[String, StructField] = fields.map(sf => sf.name -> sf).toMap

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
