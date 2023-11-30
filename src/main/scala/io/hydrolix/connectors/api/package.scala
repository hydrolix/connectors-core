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

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonInclude}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming

/**
 * These are Scala representations of the JSON schema returned by the Hydrolix API.
 */
package object api {
  @JsonIgnoreProperties(ignoreUnknown = true)
  case class HdxOutputColumn(name: String,
                         datatype: HdxColumnDatatype)

  @JsonNaming(classOf[SnakeCaseStrategy])
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(Include.NON_DEFAULT)
  case class HdxColumnDatatype(`type`: HdxValueType,
                                index: Boolean,
                              primary: Boolean,
                         indexOptions: Option[JsonNode] = None,
                               source: Option[HdxColumnSource] = None,
                               format: Option[String] = None,
                           resolution: Option[String] = None,
                              default: Option[String] = None,
                               script: Option[String] = None,
                             catchAll: Boolean = false,
                               ignore: Boolean = false,
                             elements: Option[List[HdxColumnDatatype]] = None,
                               limits: Option[HdxColumnLimits] = None)

  case class HdxColumnLimits(min: Option[JsonNode],
                             max: Option[JsonNode],
                            past: Option[String],
                          future: Option[String],
                             pad: Option[String],
                          action: Option[String])

  case class HdxColumnSource(fromInputField: Option[String], // TODO it'd be nice to make this single fromInputFields
                            fromInputFields: Option[List[String]],
                             fromInputIndex: Option[Int],
                           fromJsonPointers: Option[List[String]],
                         fromAutomaticValue: Option[String])
}
