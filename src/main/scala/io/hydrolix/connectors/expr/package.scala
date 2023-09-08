package io.hydrolix.connectors

import java.time.Instant

import io.hydrolix.connectors.types.{TimestampType, ValueType}

package object expr {
  trait Expr[+R <: Any] {
    def `type`: ValueType
    def children: List[Expr[_]]
  }

  object Now extends Expr[Instant] {
    override val `type` = TimestampType.Micros
    override val children = Nil
  }
}
