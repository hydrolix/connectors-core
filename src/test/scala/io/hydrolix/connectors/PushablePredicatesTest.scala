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

package io.hydrolix.connectors

import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connectors.expr._

//noinspection NameBooleanParameters
class PushablePredicatesTest {
  import PushdownFixture._

  @Test
  def `check pushability of simple predicates`(): Unit = {
    assertEquals("timestamp == literal is pushable", 2, HdxPushdown.pushable(pkField, None, timestampEquals1234, cols))
    assertEquals("(name:string) == 'Alex' is pushable", 2, HdxPushdown.pushable(pkField, None, nameEqualsAlex, cols))
    assertEquals("(age:uint32) == 50 is NOT pushable", 3, HdxPushdown.pushable(pkField, None, ageEquals50, cols))
  }

  @Test
  def `ANDs over pushable predicates are still pushable`(): Unit = {
    assertEquals("timestamp == literal AND nothing", 2, HdxPushdown.pushable(pkField, None, And(timestampEquals1234, BooleanLiteral.True), cols))
    assertEquals("timestamp == literal AND string == literal is pushable", 2, HdxPushdown.pushable(pkField, None, And(timestampEquals1234, nameEqualsAlex), cols))
  }

  @Test
  def `ORs over uniform pushable predicates are still pushable`(): Unit = {
    assertEquals("timestamp == literal1 OR timestamp == literal2", 2, HdxPushdown.pushable(pkField, None, Or(timestampEquals1234, timestampEquals2345), cols))
  }
}
