package io.hydrolix.connectors.expr

import io.hydrolix.connectors.`type`.Int64Type

abstract class AggregateFun[T] extends Expr[T] {
  val op: AggregateOp
}

case class Min[T : Numeric](child: Expr[T]) extends AggregateFun[T] {
  override val op = AggregateOp.Min
  override val `type` = child.`type`
  override val children = List(child)
}
case class Max[T : Numeric](child: Expr[T]) extends AggregateFun[T] {
  override val op = AggregateOp.Max
  override val `type` = child.`type`
  override val children = List(child)
}
case class Sum[T : Numeric](child: Expr[T]) extends AggregateFun[T] {
  override val op = AggregateOp.Sum
  override val `type` = child.`type`
  override val children = List(child)
}
case class Count[T](child: Expr[T]) extends AggregateFun[Long] {
  override val `type` = Int64Type
  override val op = AggregateOp.Count
  override val children = List(child)
}
case object CountStar extends AggregateFun[Long] {
  override val `type` = Int64Type
  override val op = AggregateOp.CountStar
  override val children = Nil
}
case class CountDistinct[T](child: Expr[T]) extends AggregateFun[Long] {
  override val `type` = Int64Type
  override val op = AggregateOp.CountDistinct
  override val children = List(child)
}
