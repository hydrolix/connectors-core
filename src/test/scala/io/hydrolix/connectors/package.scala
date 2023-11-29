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
