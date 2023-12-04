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

import java.util.UUID

import org.apache.hc.client5.http.classic.methods.HttpPost
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.core5.http.ClassicHttpResponse
import org.apache.hc.core5.http.io.entity.{EntityUtils, StringEntity}

import io.hydrolix.connectors.TestUtils._
import io.hydrolix.connectors.api._

object CreateTableAndTransform extends App {
  val info = connectionInfo()
  val dbName = args(0)
  val tableName = args(1)

  val api = new HdxApiSession(info)

  val project = api.database(dbName).getOrElse(sys.error(s"Database $dbName doesn't exist"))

  private val client = HttpClients.createDefault()

  val apiTable = api.table(dbName, tableName).getOrElse {
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
    println(newTableBody)

    val createTablePost = new HttpPost(info.apiUrl.resolve(s"orgs/${project.org}/projects/${project.uuid}/tables/")).also { post =>
      api.addAuthToken(post)
      post.setEntity(new StringEntity(newTableBody))
      post.setHeader("Content-Type", "application/json")
    }

    client.execute(createTablePost, { resp: ClassicHttpResponse =>
      val body = EntityUtils.toString(resp.getEntity)
      if (resp.getCode / 100 == 2) {
        JSON.objectMapper.readValue[HdxApiTable](body).also { created =>
          println(s"Table #${created.uuid} created successfully")
        }
      } else {
        println(s"Create Table response code was ${resp.getCode}: ${resp.getReasonPhrase}")
        println(s"Create Table response body was $body")
        sys.exit(1)
      }
    })
  }

  val transform = HdxTransform(
    uuid = UUID.randomUUID(),
    name = s"$dbName.$tableName",
    settings = HdxTransformSettings(
      isDefault = true,
      outputColumns = allColumns
    ),
    `type` = HdxTransformType.json,
    table = apiTable.uuid
  )

  val postTransformReq = new HttpPost(info.apiUrl.resolve(s"orgs/${project.org}/projects/${project.uuid}/tables/${apiTable.uuid}/transforms/")).also { post =>
    api.addAuthToken(post)
    val transformPostBody = JSON.objectMapper.writeValueAsString(transform)
    println(s"Transform POST body: $transformPostBody")
    post.setEntity(new StringEntity(transformPostBody))
    post.setHeader("Content-Type", "application/json")
  }

  val postTransformResp = client.execute(postTransformReq, { resp: ClassicHttpResponse =>
    val body = EntityUtils.toString(resp.getEntity)
    if (resp.getCode / 100 == 2) {
      JSON.objectMapper.readValue[HdxTransform](body).also { created =>
        println(s"Transform #${created.uuid} was created successfully")
      }
    } else {
      println(s"Create Transform response code was ${resp.getCode}: ${resp.getReasonPhrase}")
      println(s"Create Transform response body was $body")
      sys.exit(1)
    }
  })

  println(postTransformResp)
}
