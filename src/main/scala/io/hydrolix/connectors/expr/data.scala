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

/*
case class GroupAggregate(groupByExprs: List[Expr[_]], aggs: List[AggregateFun[_]], aggNames: List[Option[String]]) extends Expr[TableLiteral] {
  override val `type` = aggs.map(_.`type`)
  override val children = groupByExprs ++ aggs
}

case class TableLiteral(schema: StructType, rows: List[Map[String, Any]]) extends Literal[(StructType, List[Map[String, Any]])] {
  override val `type` = schema // TODO probably not?
  override val value = (schema, rows)
}*/
