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

import java.util.Random

import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connectors.TestUtils.christmasTreeStructNoMaps

class DataGenTest {
  @Test
  def `generate some data`(): Unit = {
    val rng = new Random()
    println(chonk(rng, 10000).length)
  }

  private def chonk(rng: Random, num: Int): String = {
    val s = new StringBuilder()
    for (_ <- 0 until num) {
      val obj = DataGen(christmasTreeStructNoMaps, false, rng)
      s.append(JSON.objectMapper.writeValueAsString(obj))
    }
    s.toString()
  }

  @Test
  def `check repeatability`(): Unit = {
    val chonk1 = chonk(new Random(1234L), 1000)
    val chonk2 = chonk(new Random(1234L), 1000)

    assertEquals(chonk1, chonk2)
  }
}
