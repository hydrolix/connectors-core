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
import javax.sql.DataSource

/**
 * All the information we need to connect to a Hydrolix cluster
 *
 * @param jdbcUrl                 the JDBC URL to connect to the Hydrolix query head,
 *                                e.g. `jdbc:clickhouse://host:port/db?ssl=true`
 * @param user                    the username to authenticate to JDBC and the Hydrolix API
 * @param password                the password for authentication to JDBC and the Hydrolix API
 * @param apiUrl                  the URL of the Hydrolix API; probably needs to end with `/config/v1/` at the moment.
 * @param partitionPrefix         string to prepend to partition paths, only needed during weird version transitions
 * @param cloudCred1              first credential for storage, required, e.g.:
 *                                  - for `gcs`, the base64(gzip(_)) of a gcp service account key file
 *                                  - for AWS, the access key ID
 * @param cloudCred2              second credential for storage, optional, e.g.:
 *                                  - for `gcs`, not required
 *                                  - for AWS, the secret key
 * @param turbineCmdDockerName    name of a Docker image/container to use to run `turbine_cmd`, in case the host OS
 *                                isn't a modern Linux distro
 * @param dataSource              [for testing] pass a DataSource here so we don't need to connect to a real ClickHouse
 * @param timestampLiteralConv    [for testing] a string like `parseDateTimeBestEffort(?)` or
 *                                `parsedatetime(?, 'yyyy-MM-dd'' ''HH:mm:ss')` to use when converting timestamp
 *                                literals to timestamp values
 */
case class HdxConnectionInfo(jdbcUrl: String,
                                user: String,
                            password: String,
                              apiUrl: URI,
                     partitionPrefix: Option[String],
                          cloudCred1: String,
                          cloudCred2: Option[String],
                turbineCmdDockerName: Option[String],
     @transient           dataSource: Option[DataSource] = None,
     @transient timestampLiteralConv: Option[String] = None,
                           extraOpts: Map[String, String] = Map())
{
  @transient lazy val asMap: Map[String, String] = {
    import HdxConnectionInfo._

    Map(
      OPT_JDBC_URL -> jdbcUrl,
      OPT_USERNAME -> user,
      OPT_PASSWORD -> password,
      OPT_API_URL -> apiUrl.toString,
      OPT_CLOUD_CRED_1 -> cloudCred1,
    ) ++ extraOpts ++ List(
      turbineCmdDockerName.map(OPT_TURBINE_CMD_DOCKER -> _),
      cloudCred2.map(OPT_CLOUD_CRED_2 -> _),
      partitionPrefix.map(OPT_PARTITION_PREFIX -> _),
    ).flatten
  }
}

//noinspection ScalaWeakerAccess
object HdxConnectionInfo {
  val OPT_PROJECT_NAME = "project_name"
  val OPT_TABLE_NAME = "table_name"
  val OPT_JDBC_URL = "jdbc_url"
  val OPT_USERNAME = "username"
  val OPT_PASSWORD = "password"
  val OPT_API_URL = "api_url"
  val OPT_PARTITION_PREFIX = "partition_prefix"
  val OPT_CLOUD_CRED_1 = "cloud_cred_1"
  val OPT_CLOUD_CRED_2 = "cloud_cred_2"
  val OPT_STORAGE_CLOUD = "storage_cloud"
  val OPT_STORAGE_REGION = "storage_region"
  val OPT_STORAGE_BUCKET_NAME = "storage_bucket_name"
  val OPT_STORAGE_BUCKET_PATH = "storage_bucket_path"
  val OPT_STORAGE_ENDPOINT_URI = "storage_endpoint_uri"
  val OPT_QUERY_MODE = "query_mode"
  val OPT_TURBINE_CMD_DOCKER = "turbine_cmd_docker"

  def req(options: Map[String, String], name: String): String = {
    options.getOrElse(name, sys.error(s"$name is required"))
  }

  def opt(options: Map[String, String], name: String): Option[String] = {
    options.get(name)
  }

  def fromOpts(options: Map[String, String]): HdxConnectionInfo = {
    val url = req(options, OPT_JDBC_URL)
    val user = req(options, OPT_USERNAME)
    val pass = req(options, OPT_PASSWORD)
    val apiUrl = new URI(req(options, OPT_API_URL))
    val partitionPrefix = opt(options, OPT_PARTITION_PREFIX)
    val cloudCred1 = req(options, OPT_CLOUD_CRED_1)
    val cloudCred2 = opt(options, OPT_CLOUD_CRED_2)
    val turbineCmdDocker = opt(options, OPT_TURBINE_CMD_DOCKER)

    val extra = options - OPT_JDBC_URL - OPT_USERNAME - OPT_PASSWORD - OPT_API_URL - OPT_PARTITION_PREFIX - OPT_CLOUD_CRED_1 - OPT_CLOUD_CRED_2 - OPT_TURBINE_CMD_DOCKER
    HdxConnectionInfo(url, user, pass, apiUrl, partitionPrefix, cloudCred1, cloudCred2, turbineCmdDocker, extraOpts = extra)
  }

  def getRequiredEnv(name: String): String = {
    Option(System.getenv(name)).getOrElse(sys.error(s"$name is required"))
  }

  def fromEnv(): HdxConnectionInfo = {
    val jdbcUrl = getRequiredEnv("HDX_JDBC_URL")
    val apiUrl = getRequiredEnv("HDX_API_URL")
    val user = getRequiredEnv("HDX_USER")
    val pass = getRequiredEnv("HDX_PASSWORD")
    val cloudCred1 = getRequiredEnv("HDX_CLOUD_CRED_1")
    val cloudCred2 = Option(System.getenv("HDX_CLOUD_CRED_2"))
    val dockerImageName = Option(System.getenv("HDX_DOCKER_IMAGE"))
    val endpointUri = Option(System.getenv("HDX_STORAGE_ENDPOINT"))
    val partitionPrefix = Option(System.getenv("HDX_STORAGE_PARTITION_PREFIX"))

    val extra = Map() ++
      endpointUri.map(uri => HdxConnectionInfo.OPT_STORAGE_ENDPOINT_URI -> uri)

    HdxConnectionInfo(jdbcUrl, user, pass, new URI(apiUrl), partitionPrefix, cloudCred1, cloudCred2, dockerImageName, None, None, extra)
  }
}
