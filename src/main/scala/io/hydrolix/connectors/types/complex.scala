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
