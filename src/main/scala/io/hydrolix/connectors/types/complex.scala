package io.hydrolix.connectors.types

sealed trait ComplexType extends ValueType

case class ArrayType(elementType: ValueType, elementsNullable: Boolean = false) extends ComplexType

case class MapType(keyType: ValueType, valueType: ValueType, valuesNullable: Boolean = false) extends ComplexType

case class StructField(name: String, `type`: ValueType, nullable: Boolean = false)

case class StructType(fields: StructField*) extends ComplexType
