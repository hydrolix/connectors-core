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

import io.hydrolix.connectors.types.{BooleanType, ValueType}

case class GetField[T](name: String, `type`: ValueType) extends Expr[T] {
  override val children = Nil
}

case class IsNull[T](expr: Expr[T]) extends Expr[Boolean] {
  override val `type` = BooleanType
  override val children = List(expr)
}

case class In[T](left: Expr[T], rights: Expr[Seq[T]]) extends Expr[Boolean] {
  override val `type` = BooleanType
  override val children = List(left, rights)
}
