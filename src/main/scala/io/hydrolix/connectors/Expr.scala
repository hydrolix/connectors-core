//noinspection TypeAnnotation
package io.hydrolix.connectors

import java.time.Instant

trait Expr[+R <: Any] {
  def `type`: ValueType
  def children: List[Expr[_]]
}

case class GetField[T](name: String, `type`: ValueType) extends Expr[T] {
  override val children = Nil
}

case class IsNull[T](expr: Expr[T]) extends Expr[Boolean] {
  override val `type` = BooleanType
  override val children = List(expr)
}

object Now extends Expr[Instant] {
  override val `type` = TimestampType.Millis
  override val children = Nil
}

case class In[T](left: Expr[T], rights: Expr[List[T]]) extends Expr[Boolean] {
  override val `type` = BooleanType
  override val children = List(left, rights)
}

case class Not(expr: Expr[Boolean]) extends Expr[Boolean] {
  override val `type` = BooleanType
  override val children = List(expr)
}

abstract class Comparison[T] extends Expr[Boolean] {
  override val `type` = BooleanType
  override val children = List(left, right)

  val left: Expr[T]
  val right: Expr[T]
  val op: ComparisonOp
}
object Comparison {
  def apply[T](left: Expr[T], op: ComparisonOp, right: Expr[T]): Expr[Boolean] = {
    op match {
      case ComparisonOp.EQ => Equal(left, right)
      case ComparisonOp.NE => NotEqual(left, right)
      case ComparisonOp.LT => LessThan(left, right)
      case ComparisonOp.LE => LessEqual(left, right)
      case ComparisonOp.GT => GreaterThan(left, right)
      case ComparisonOp.GE => GreaterEqual(left, right)
    }
  }


  def unapply(expr: Expr[Boolean]): Option[(Expr[_], ComparisonOp, Expr[_])] = {
    expr match {
      case cmp: Comparison[_] =>
        Some(cmp.left, cmp.op, cmp.right)
      case _ =>
        None
    }
  }
}

case class Equal[T](left: Expr[T], right: Expr[T]) extends Comparison[T] {
  override val op = ComparisonOp.EQ
}
case class NotEqual[T](left: Expr[T], right: Expr[T]) extends Comparison[T] {
  override val op = ComparisonOp.NE
}
case class GreaterThan[T](left: Expr[T], right: Expr[T]) extends Comparison[T] {
  override val op = ComparisonOp.GT
}
case class GreaterEqual[T](left: Expr[T], right: Expr[T]) extends Comparison[T] {
  override val op = ComparisonOp.GE
}
case class LessThan[T](left: Expr[T], right: Expr[T]) extends Comparison[T] {
  override val op = ComparisonOp.LT
}
case class LessEqual[T](left: Expr[T], right: Expr[T]) extends Comparison[T] {
  override val op = ComparisonOp.LE
}

case class And(children: List[Expr[Boolean]]) extends Expr[Boolean] {
  override val `type` = BooleanType
}
case class Or(children: List[Expr[Boolean]]) extends Expr[Boolean] {
  override val `type` = BooleanType
}

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
