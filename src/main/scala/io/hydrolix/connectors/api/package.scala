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
                               source: Option[JsonNode] = None,
                               format: Option[String] = None,
                           resolution: Option[String] = None,
                              default: Option[String] = None,
                               script: Option[String] = None,
                             catchAll: Boolean = false,
                               ignore: Boolean = false,
                             elements: Option[List[HdxColumnDatatype]] = None)
}
