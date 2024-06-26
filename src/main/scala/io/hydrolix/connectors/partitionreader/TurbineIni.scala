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

package io.hydrolix.connectors.partitionreader

import scala.util.Using

import com.google.common.io.ByteStreams
import com.typesafe.scalalogging.Logger

import io.hydrolix.connectors.api.HdxStorageSettings

object TurbineIni {
  private val log = Logger(getClass)

  private val template = Using.resource(getClass.getResourceAsStream("/turbine_template.ini")) { stream =>
    new String(ByteStreams.toByteArray(stream), "UTF-8")
  }

  def apply(storage: HdxStorageSettings,
         cloudCred1: String,
         cloudCred2: Option[String],
          hdxFsPath: String)
                   : String =
  {
    val (creds, storageInfo) = storage.cloud match {
      case "gcp" | "gcs" =>
        (
          """### Connectors: GCS credentials
            |fs.gcs.credentials.method = service_account
            |fs.gcs.credentials.json_credentials_file = %CREDS_FILE%"""
            .stripMargin,

          s"""### Connectors: GCS storage info
             |fs.id.default.type = gcs
             |fs.id.default.gcs.region = ${storage.region}
             |fs.id.default.gcs.storage.bucket_name = ${storage.bucketName}
             |fs.id.default.gcs.storage.bucket_path = ${storage.bucketPath}"""
            .stripMargin
        )

      case "aws" =>
        val endpointUrlOverride = storage.endpoint
          .map(url => s"fs.id.default.aws.s3.endpoint = $url\n")
          .getOrElse("\n")
        (
          s"""### Connectors: AWS credentials
             |fs.aws.credentials.method = static
             |fs.aws.credentials.access_key = $cloudCred1
             |fs.aws.credentials.secret_key = ${cloudCred2.getOrElse(sys.error("cloud_cred_2 is required for AWS"))}"""
            .stripMargin,

          s"""### Connectors: AWS storage info
             |fs.id.default.type = s3
             |fs.id.default.aws.region = ${storage.region}
             |fs.id.default.aws.s3.bucket_name = ${storage.bucketName}
             |fs.id.default.aws.s3.bucket_path = ${storage.bucketPath}"""
            .stripMargin + endpointUrlOverride
        )

      case "azure" =>
        (
          s"""### Connectors: Azure credentials
             |fs.azure.credentials.account_name = $cloudCred1
             |fs.azure.credentials.shared_key = ${cloudCred2.getOrElse(sys.error("cloud_cred_2 is required for Azure"))}"""
            .stripMargin,
          s"""### Connectors: Azure storage info
             |fs.id.default.type = azure
             |fs.id.default.azure.storage.container_name = ${storage.bucketName}
             |fs.id.default.azure.storage.container_path = ${storage.bucketPath}"""
            .stripMargin
        )

      case other =>
        log.warn(s"Don't know how to generate turbine.ini for storage type $other; leaving it blank and hoping for the best")
        (
          s"# unknown credentials for $other",
          s"# unknown storage info $other"
        )
    }

    val sslEnabled = if (storage.endpoint.isDefined) {
      ""
    } else {
      """### Connectors: enable SSL when storage endpoint URL isn't defined
        |fs.http.ssl.enabled = true""".stripMargin
    }

    template
      .replace("%CLOUD_CREDS%", creds)
      .replace("%CLOUD_STORAGE_INFO%", storageInfo)
      .replace("%TMP_HDXFS%", hdxFsPath)
      .replace("%HTTP_SSL_ENABLED%", sslEnabled)
  }
}
