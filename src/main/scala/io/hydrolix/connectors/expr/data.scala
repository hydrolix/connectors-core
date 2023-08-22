package io.hydrolix.connectors.expr

import io.hydrolix.connectors.`type`.{BooleanType, ValueType}

case class GetField[T](name: String, `type`: ValueType) extends Expr[T] {
  override val children = Nil
}

case class IsNull[T](expr: Expr[T]) extends Expr[Boolean] {
  override val `type` = BooleanType
  override val children = List(expr)
}

case class In[T](left: Expr[T], rights: Expr[List[T]]) extends Expr[Boolean] {
  override val `type` = BooleanType
  override val children = List(left, rights)
}
