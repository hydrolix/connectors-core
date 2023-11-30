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

import io.hydrolix.connectors.types.Int64Type

// TODO Min, Max and Sum should be nullable but we don't have such a type yet
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
