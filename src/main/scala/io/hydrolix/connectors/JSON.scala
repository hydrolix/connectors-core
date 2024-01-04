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

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, MapperFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

object JSON {
  val objectMapper: JsonMapper with ClassTagExtensions = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .addModule(new JavaTimeModule())
    .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS) // For HdxValueType mostly, since `double` and `boolean` are taken
    .build() :: ClassTagExtensions

  implicit class ObjectNodeStuff(val obj: ObjectNode) extends AnyVal {
    def asMap: Map[String, JsonNode] = {
      obj.fieldNames().asScala.map { k =>
        k -> obj.get(k)
      }.toMap
    }
  }
}
