package io.hydrolix.connectors

package object types {
  trait ValueType extends Serializable

  object AnyType extends ValueType
}
