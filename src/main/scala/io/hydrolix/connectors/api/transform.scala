package io.hydrolix.connectors.api

import java.net.URL
import java.time.Instant
import java.util.UUID

import com.fasterxml.jackson.annotation.{JsonFormat, JsonIgnoreProperties, JsonInclude, OptBoolean}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTransform(name: String,
                 description: Option[String] = None,
                        uuid: UUID,
@JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'", timezone = "UTC", lenient = OptBoolean.TRUE)
                     created: Instant = Instant.now(),
@JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'", timezone = "UTC", lenient = OptBoolean.TRUE)
                    modified: Instant = Instant.now(),
                    settings: HdxTransformSettings,
                         url: Option[URL] = None, // unset on create
                      `type`: HdxTransformType,
                       table: UUID)

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonInclude(Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTransformSettings(isDefault: Boolean,
                             sqlTransform: Option[String] = None,
                               nullValues: Option[List[String]] = None,
                               sampleData: Option[JsonNode] = None,
                            outputColumns: List[HdxOutputColumn],
                              compression: String = "none",
                            formatDetails: Option[HdxTransformFormatDetails] = None)

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTransformFormatDetails(delimiter: Option[String], // TODO put the CSV stuff in a separate type
                                        escape: Option[String],
                                      skipHead: Option[Int],
                                         quote: Option[String],
                                       comment: Option[String],
                                  skipComments: Option[Boolean],
                                 windowsEnding: Option[Boolean],
                                    flattening: HdxTransformFormatFlattening)

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTransformFormatFlattening(depth: Int,
                                       active: Boolean,
                        mapFlatteningStrategy: Option[HdxTransformFlatteningStrategy],
                      sliceFlatteningStrategy: Option[HdxTransformFlatteningStrategy])

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTransformFlatteningStrategy(left: Option[String], right: Option[String])
