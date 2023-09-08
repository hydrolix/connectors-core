package io.hydrolix.connectors.expr

import io.hydrolix.connectors.types.BooleanType

sealed abstract class Comparison[T] extends Expr[Boolean] {
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
