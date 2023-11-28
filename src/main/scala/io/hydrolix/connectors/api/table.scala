package io.hydrolix.connectors.api

import java.net.URL
import java.time.Instant
import java.util.UUID

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxApiTable(project: UUID,
                          name: String,
                   description: Option[String],
                          uuid: UUID,
                       created: Instant,
                      modified: Instant,
                      settings: HdxTableSettings,
                           url: URL,
                        `type`: String,
                    primaryKey: Option[String])

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTableSettings(stream: HdxTableStreamSettings,
                               age: HdxTableSettingsAge,
                            reaper: HdxTableSettingsAge,
                             merge: HdxTableSettingsMerge,
                        autoingest: List[HdxTableSettingsAutoIngest],
                          sortKeys: List[String],
                          shardKey: Option[String],
                     maxFutureDays: Int,
                           summary: Option[HdxTableSettingsSummary],
                             scale: Option[HdxTableSettingsScale],
                   maxRequestBytes: Option[Long],
                        storageMap: Option[HdxTableSettingsStorageMap])

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTableStreamSettings(tokenList: List[String],
                       hotDataMaxAgeMinutes: Int,
                 hotDataMaxActivePartitions: Int,
                 hotDataMaxRowsPerPartition: Long,
              hotDataMaxMinutesPerPartition: Long,
                      hotDataMaxOpenSeconds: Long,
                      hotDataMaxIdleSeconds: Long,
                         coldDataMaxAgeDays: Int,
                coldDataMaxActivePartitions: Int,
                coldDataMaxRowsPerPartition: Long,
             coldDataMaxMinutesPerPartition: Int,
                     coldDataMaxOpenSeconds: Int,
                     coldDataMaxIdleSeconds: Int,
                        messageQueueMaxRows: Option[Int],
                           tokenAuthEnabled: Option[Boolean])

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxTableSettingsAge(maxAgeDays: Int)

@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTableSettingsMerge(enabled: Boolean,
                                   pools: Map[String, String] = Map(),
                                     sql: Option[String])


@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTableSettingsStorageMap(defaultStorageId: UUID,
                                            columnName: String,
                                    columnValueMapping: Map[UUID, List[JsonNode]])

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxTableSettingsSummary(sql: String, enabled: Boolean)

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

