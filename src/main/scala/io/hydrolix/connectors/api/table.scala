package io.hydrolix.connectors.api

import java.net.URL
import java.time.Instant
import java.util.UUID

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonFormat, JsonIgnoreProperties, JsonInclude, OptBoolean}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonInclude(Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxApiTable(project: UUID,
                          name: String,
                   description: Option[String] = None,
                          uuid: UUID,
                       @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'", timezone = "UTC", lenient = OptBoolean.TRUE)
                       created: Instant = Instant.now(),
                       @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'", timezone = "UTC", lenient = OptBoolean.TRUE)
                      modified: Instant = Instant.now(),
                      settings: HdxTableSettings,
                           url: Option[URL] = None,
                        `type`: Option[String],
                    primaryKey: Option[String] = None)

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonInclude(Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTableSettings(stream: HdxTableStreamSettings,
                               age: HdxTableSettingsAge = HdxTableSettingsAge(180),
                            reaper: HdxTableSettingsAge = HdxTableSettingsAge(1),
                             merge: HdxTableSettingsMerge = HdxTableSettingsMerge(true),
                        autoingest: List[HdxTableSettingsAutoIngest] = Nil,
                          sortKeys: List[String] = Nil,
                          shardKey: Option[String] = None,
                     maxFutureDays: Int = 0,
                           summary: Option[HdxTableSettingsSummary],
                             scale: HdxTableSettingsScale,
                   maxRequestBytes: Long = -1,
                        storageMap: Option[HdxTableSettingsStorageMap] = None)

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTableStreamSettings(tokenList: List[String] = Nil,
                       hotDataMaxAgeMinutes: Int = 5,
                 hotDataMaxActivePartitions: Int = 3,
                 hotDataMaxRowsPerPartition: Long = 2097152,
              hotDataMaxMinutesPerPartition: Long = 5,
                      hotDataMaxOpenSeconds: Long = 10,
                      hotDataMaxIdleSeconds: Long = 10,
                         coldDataMaxAgeDays: Int = 365,
                coldDataMaxActivePartitions: Int = 50,
                coldDataMaxRowsPerPartition: Long = 2097152,
             coldDataMaxMinutesPerPartition: Int = 60,
                     coldDataMaxOpenSeconds: Int = 60,
                     coldDataMaxIdleSeconds: Int = 30,
                        messageQueueMaxRows: Option[Int] = Some(0),
                           tokenAuthEnabled: Option[Boolean] = Some(false))

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxTableSettingsAge(maxAgeDays: Int)

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_DEFAULT)
case class HdxTableSettingsMerge(enabled: Boolean,
                                   pools: Option[Map[String, String]] = None,
                                     sql: String = "")

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTableSettingsStorageMap(defaultStorageId: UUID,
                                            columnName: String,
                                    columnValueMapping: Map[UUID, List[JsonNode]])

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTableSettingsSummary(sql: String = "", enabled: Boolean)

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTableSettingsScale(expectedTbPerDay: Long)

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTableSettingsAutoIngest(enabled: Boolean,
                                       source: String,
                                      pattern: String,
                          maxRowsPerPartition: Long,
                       maxMinutesPerPartition: Long,
                          maxActivePartitions: Int,
                                       dryRun: Boolean,
                                         name: Option[String],
                                    transform: Option[String])

