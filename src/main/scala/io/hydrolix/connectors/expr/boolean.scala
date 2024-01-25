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

//noinspection TypeAnnotation
package io.hydrolix.connectors.expr

import io.hydrolix.connectors.types.BooleanType

case class Not(expr: Expr[Boolean]) extends Expr[Boolean] {
  override val `type` = BooleanType
  override val children = List(expr)

  override def simplify(): Expr[Boolean] = {
    this match {
      case Not(BooleanLiteral.True)  => BooleanLiteral.False                 // not(true) => false
      case Not(BooleanLiteral.False) => BooleanLiteral.True                  // not(false) => true
      case Not(Not(grandchild))      => grandchild.simplify()                // not(not(any)) => any
      case Not(Equal(l, r))          => NotEqual(l.simplify(), r.simplify()) // not(equal(l, r)) => notequal(l, r)
      case _ => Not(expr.simplify())
    }
  }
}

case class And(left: Expr[Boolean], right: Expr[Boolean]) extends Expr[Boolean] {
  override val `type` = BooleanType
  override def children: List[Expr[_]] = List(left, right)
  override def simplify(): Expr[Boolean] = {
    val ls = left.simplify()
    val rs = right.simplify()

    (ls, rs) match {
      case (BooleanLiteral.False, _) => BooleanLiteral.False                 // false && any => false
      case (_, BooleanLiteral.False) => BooleanLiteral.False                 // any && false => false
      case (BooleanLiteral.True, BooleanLiteral.True) => BooleanLiteral.True // true && true => true
      case (BooleanLiteral.True, r) => r                                     // true && r => r
      case (l, BooleanLiteral.True) => l                                     // l && true => l
      case (l, r) if l == r => l
      case _ => And(ls, rs)
    }
  }
}

case class Or(left: Expr[Boolean], right: Expr[Boolean]) extends Expr[Boolean] {
  override val `type` = BooleanType
  override def children: List[Expr[_]] = List(left, right)

  override def simplify(): Expr[Boolean] = {
    val ls = left.simplify()
    val rs = right.simplify()

    (ls, rs) match {
      case (BooleanLiteral.False, BooleanLiteral.False) => BooleanLiteral.False // false || false => false
      case (BooleanLiteral.True, BooleanLiteral.True) => BooleanLiteral.True    // true || true => true
      case (_, BooleanLiteral.True) => BooleanLiteral.True                      // any || true => true
      case (BooleanLiteral.True, _) => BooleanLiteral.True                      // true || any => true
      case (BooleanLiteral.False, r) => r                                       // false || r => r
      case (l, BooleanLiteral.False) => l                                       // l || false => l
      case (l, r) if l == r => l
      case _ => Or(ls, rs)
    }
  }
}
