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
