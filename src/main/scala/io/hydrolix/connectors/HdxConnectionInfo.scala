package io.hydrolix.connectors


import java.net.URI

/**
 * All the information we need to connect to a Hydrolix cluster
 *
 * @param jdbcUrl              the JDBC URL to connect to the Hydrolix query head,
 *                             e.g. `jdbc:clickhouse://host:port/db?ssl=true`
 * @param user                 the username to authenticate to JDBC and the Hydrolix API
 * @param password             the password for authentication to JDBC and the Hydrolix API
 * @param apiUrl               the URL of the Hydrolix API; probably needs to end with `/config/v1/` at the moment.
 * @param partitionPrefix      string to prepend to partition paths, only needed during weird version transitions
 * @param cloudCred1           first credential for storage, required, e.g.:
 *                               - for `gcs`, the base64(gzip(_)) of a gcp service account key file
 *                               - for AWS, the access key ID
 * @param cloudCred2           second credential for storage, optional, e.g.:
 *                               - for `gcs`, not required
 *                               - for AWS, the secret key
 * @param turbineCmdDockerName name of a Docker image/container to use to run `turbine_cmd`, in case the host OS isn't
 *                             a modern Linux distro
 */
case class HdxConnectionInfo(jdbcUrl: String,
                                user: String,
                            password: String,
                              apiUrl: URI,
                     partitionPrefix: Option[String],
                          cloudCred1: String,
                          cloudCred2: Option[String],
                turbineCmdDockerName: Option[String])
{
  @transient lazy val asMap: Map[String, String] = {
    import HdxConnectionInfo._

    Map(
      OPT_JDBC_URL -> jdbcUrl,
      OPT_USERNAME -> user,
      OPT_PASSWORD -> password,
      OPT_API_URL -> apiUrl.toString,
      OPT_CLOUD_CRED_1 -> cloudCred1,
    ) ++ List(
      turbineCmdDockerName.map(OPT_TURBINE_CMD_DOCKER -> _),
      cloudCred2.map(OPT_CLOUD_CRED_2 -> _),
      partitionPrefix.map(OPT_PARTITION_PREFIX -> _)
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

    HdxConnectionInfo(url, user, pass, apiUrl, partitionPrefix, cloudCred1, cloudCred2, turbineCmdDocker)
  }
}

