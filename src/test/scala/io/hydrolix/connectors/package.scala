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

import java.net.URI

object TestUtils {
  def connectionInfo() = {
    val jdbcUrl = System.getenv("HDX_JDBC_URL")
    val apiUrl = System.getenv("HDX_API_URL")
    val user = System.getenv("HDX_USER")
    val pass = System.getenv("HDX_PASSWORD")
    val cloudCred1 = System.getenv("HDX_CLOUD_CRED_1")
    val cloudCred2 = Option(System.getenv("HDX_CLOUD_CRED_2"))
    val dockerImageName = Option(System.getenv("HDX_DOCKER_IMAGE"))

    HdxConnectionInfo(jdbcUrl, user, pass, new URI(apiUrl), None, cloudCred1, cloudCred2, dockerImageName)
  }
}
