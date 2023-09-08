//noinspection TypeAnnotation
package io.hydrolix.connectors.expr

import io.hydrolix.connectors.types.BooleanType

case class Not(expr: Expr[Boolean]) extends Expr[Boolean] {
  override val `type` = BooleanType
  override val children = List(expr)
}

case class And(children: List[Expr[Boolean]]) extends Expr[Boolean] {
  override val `type` = BooleanType
}
case class Or(children: List[Expr[Boolean]]) extends Expr[Boolean] {
  override val `type` = BooleanType
}
