package io.hydrolix.connectors.api

import java.net.URI
import java.time.Instant
import java.util.UUID

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxView(uuid: UUID,
                   name: String,
            description: Option[String],
                created: Instant,
               modified: Instant,
               settings: HdxViewSettings,
                    url: URI,
                  table: UUID)

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxViewSettings(isDefault: Boolean,
                       outputColumns: List[HdxOutputColumn])
