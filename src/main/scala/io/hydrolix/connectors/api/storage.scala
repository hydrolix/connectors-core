package io.hydrolix.connectors.api

import java.net.URL
import java.time.Instant
import java.util.UUID

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxStorage(name: String,
                       org: UUID,
               description: Option[String],
                      uuid: UUID,
                       url: URL,
                   created: Instant,
                  modified: Instant,
                  settings: HdxStorageSettings)

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxStorageSettings(isDefault: Boolean,
                             bucketName: String,
                             bucketPath: String,
                                 region: String,
                                  cloud: String)
