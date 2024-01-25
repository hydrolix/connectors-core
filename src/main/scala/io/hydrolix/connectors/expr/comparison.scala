/*
 * Copyright (c) 2023 Hydrolix Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hydrolix.connectors.expr

import io.hydrolix.connectors.types.BooleanType

//noinspection TypeAnnotation
sealed abstract class Comparison[T] extends Expr[Boolean] {
  override val `type` = BooleanType

  val op: ComparisonOp
  val left: Expr[T]
  val right: Expr[T]

  override val children = List(left, right)

  override def simplify(): Expr[Boolean] = {
    Comparison(left.simplify(), op, right.simplify())
  }
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

  // `foo == foo` is always true only when foo is pure, and we have no such notion at the moment.
  override def simplify(): Expr[Boolean] = {
    val ls = left.simplify()
    val rs = right.simplify()

    (ls, rs) match {
      case (lit1: Literal[_], lit2: Literal[_]) if lit1 == lit2 => BooleanLiteral.True
      case (lit1: Literal[_], lit2: Literal[_]) if lit1 != lit2 => BooleanLiteral.False
      case (gf1: GetField[_], gf2: GetField[_]) if gf1 == gf2 => BooleanLiteral.True
      case (gf1: GetField[_], gf2: GetField[_]) if gf1 != gf2 => BooleanLiteral.False
      case _ => Equal(ls, rs)
    }
  }
}
case class NotEqual[T](left: Expr[T], right: Expr[T]) extends Comparison[T] {
  override val op = ComparisonOp.NE

  // `foo != foo` is always true only when foo is pure, and we have no such notion at the moment.
  override def simplify(): Expr[Boolean] = {
    val ls = left.simplify()
    val rs = right.simplify()

    (ls, rs) match {
      case (lit1: Literal[_], lit2: Literal[_]) if lit1 != lit2 => BooleanLiteral.True
      case (lit1: Literal[_], lit2: Literal[_]) if lit1 == lit2 => BooleanLiteral.False
      case (gf1: GetField[_], gf2: GetField[_]) if gf1 != gf2 => BooleanLiteral.True
      case (gf1: GetField[_], gf2: GetField[_]) if gf1 == gf2 => BooleanLiteral.False
      case _ => NotEqual(ls, rs)
    }
  }
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

object Between {
  def apply[T](what: Expr[T], lower: Expr[T], upper: Expr[T]): Expr[Boolean] = {
    And(GreaterEqual(what, lower), LessEqual(what, upper))
  }
}
