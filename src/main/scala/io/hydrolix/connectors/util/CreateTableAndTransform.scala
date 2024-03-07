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

package io.hydrolix.connectors.util

import java.util.UUID

import com.typesafe.scalalogging.Logger
import org.apache.hc.client5.http.classic.methods.HttpPost
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.core5.http.ClassicHttpResponse
import org.apache.hc.core5.http.io.entity.{EntityUtils, StringEntity}

import io.hydrolix.connectors.api._
import io.hydrolix.connectors.{Etc, HdxApiSession, HdxConnectionInfo, JSON}

object CreateTableAndTransform {
  private val logger = Logger(getClass)
  private val client = HttpClients.createDefault()

  def apply(info: HdxConnectionInfo, dbName: String, tableName: String, transform: HdxTransform): Unit = {
    val api = new HdxApiSession(info)

    val project = api.database(dbName).getOrElse(sys.error(s"Database $dbName doesn't exist"))

    val apiTable = api.table(dbName, tableName).getOrElse(createTable(info, api, project, tableName))

    val postTransformReq = new HttpPost(info.apiUrl.resolve(s"orgs/${project.org}/projects/${project.uuid}/tables/${apiTable.uuid}/transforms/")).also { post =>
      api.addAuthToken(post)
      val transformPostBody = JSON.objectMapper.writeValueAsString(transform.copy(table = apiTable.uuid))
      logger.info(s"Transform POST body: $transformPostBody")
      post.setEntity(new StringEntity(transformPostBody))
      post.setHeader("Content-Type", "application/json")
    }

    val postTransformResp = client.execute(postTransformReq, { resp: ClassicHttpResponse =>
      val body = EntityUtils.toString(resp.getEntity)
      if (resp.getCode / 100 == 2) {
        JSON.objectMapper.readValue[HdxTransform](body).also { created =>
          logger.info(s"Transform #${created.uuid} was created successfully")
        }
      } else {
        logger.info(s"Create Transform response code was ${resp.getCode}: ${resp.getReasonPhrase}")
        logger.info(s"Create Transform response body was $body")
        sys.exit(1)
      }
    })

    logger.info(postTransformResp.toString)
  }

  private def createTable(info: HdxConnectionInfo, api: HdxApiSession, project: HdxProject, tableName: String): HdxApiTable = {
    val newTable = HdxApiTable(
      uuid = UUID.randomUUID(),
      project = project.uuid,
      name = tableName,
      settings = HdxTableSettings(
        scale = HdxTableSettingsScale(1),
        stream = HdxTableStreamSettings(),
        summary = None
      ),
      `type` = None
    )

    val newTableBody = JSON.objectMapper.writeValueAsString(newTable)
    logger.info(newTableBody)

    val createTablePost = new HttpPost(info.apiUrl.resolve(s"orgs/${project.org}/projects/${project.uuid}/tables/")).also { post =>
      api.addAuthToken(post)
      post.setEntity(new StringEntity(newTableBody))
      post.setHeader("Content-Type", "application/json")
    }

    client.execute(createTablePost, { resp: ClassicHttpResponse =>
      val body = EntityUtils.toString(resp.getEntity)
      if (resp.getCode / 100 == 2) {
        JSON.objectMapper.readValue[HdxApiTable](body).also { created =>
          logger.info(s"Table #${created.uuid} created successfully")
        }
      } else {
        logger.info(s"Create Table response code was ${resp.getCode}: ${resp.getReasonPhrase}")
        logger.info(s"Create Table response body was $body")
        sys.exit(1)
      }
    })
  }
}
