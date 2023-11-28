package io.hydrolix.connectors.api

import java.net.URI
import java.time.Instant
import java.util.UUID

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.JsonNode

@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxProject(uuid: UUID,
                      name: String,
                       org: UUID,
               description: Option[String],
                       url: URI,
                   created: Instant,
                  modified: Instant,
                  settings: HdxProjectSettings)

@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxProjectSettings(blob: Option[JsonNode])
