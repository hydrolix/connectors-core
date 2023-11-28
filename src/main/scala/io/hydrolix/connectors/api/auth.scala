package io.hydrolix.connectors.api

import java.util.UUID

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxLoginRequest(username: String,
                           password: String)

@JsonNaming(classOf[SnakeCaseStrategy])
@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxLoginRespAuthToken(accessToken: String,
                                   expiresIn: Long,
                                   tokenType: String)

@JsonIgnoreProperties(ignoreUnknown = true)
case class HdxLoginRespOrg(uuid: UUID,
                           name: String,
                         `type`: String,
                          cloud: String,
                     kubernetes: Boolean)

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxLoginResponse(uuid: UUID,
                           email: String,
                            orgs: List[HdxLoginRespOrg],
                          groups: List[String],
                       authToken: HdxLoginRespAuthToken,
                     isSuperuser: Option[Boolean],
                           roles: List[String])
