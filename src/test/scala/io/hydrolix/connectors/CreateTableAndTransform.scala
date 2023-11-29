package io.hydrolix.connectors

import java.util.UUID

import org.apache.hc.client5.http.classic.methods.HttpPost
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.core5.http.ClassicHttpResponse
import org.apache.hc.core5.http.io.entity.{EntityUtils, StringEntity}

import io.hydrolix.connectors.TestUtils.connectionInfo
import io.hydrolix.connectors.api._

object CreateTableAndTransform extends App {
  val info = connectionInfo()
  val dbName = args(0)
  val tableName = args(1)

  val api = new HdxApiSession(info)

  val project = api.database(dbName).getOrElse(sys.error(s"Database $dbName doesn't exist"))

  private val client = HttpClients.createDefault()

  val apiTableOut = api.table(dbName, tableName).getOrElse {
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

  val scalarColumns = HdxValueType.values().toList.filter(vt => vt.isScalar && vt != HdxValueType.DateTime64).map { vt =>
    HdxOutputColumn(
      vt.getHdxName,
      HdxColumnDatatype(
        `type` = vt,
        index = vt != HdxValueType.Double,
        primary = vt == HdxValueType.DateTime,
        resolution = vt match {
          case HdxValueType.DateTime => Some("ms")
          case HdxValueType.Epoch => Some("ms")
          case _ => None
        },
        format = vt match {
          case HdxValueType.DateTime => Some("2006-01-02T15:04:05.999Z")
          case HdxValueType.Epoch => Some("ms")
          case _ => None
        }
      )
    )
  }

  val arrayColumns = scalarColumns.map { oc =>
    HdxOutputColumn(
      oc.name + "[]",
      HdxColumnDatatype(
        `type` = HdxValueType.Array,
        index = oc.datatype.index,
        primary = false,
        elements = Some(List(oc.datatype))
      )
    )
  }

  val mapColumns = scalarColumns.map { oc =>
    HdxOutputColumn(
      oc.name + "{}",
      HdxColumnDatatype(
        `type` = HdxValueType.Map,
        index = oc.datatype.index,
        primary = false,
        elements = Some(List(
          HdxColumnDatatype(HdxValueType.String, index = true, primary = false),
          oc.datatype
        ))
      )
    )
  }

  val nestedArrayColumns = arrayColumns.map { oc =>
    HdxOutputColumn(
      oc.name + "[]",
      HdxColumnDatatype(
        `type` = HdxValueType.Array,
        index = oc.datatype.index,
        primary = false,
        elements = Some(List(
          oc.datatype
        ))
      )
    )
  }

  val transform = HdxTransform(
    uuid = UUID.randomUUID(),
    name = s"$dbName.$tableName",
    settings = HdxTransformSettings(
      isDefault = true,
      outputColumns = scalarColumns ++ arrayColumns ++ mapColumns ++ nestedArrayColumns
    ),
    `type` = HdxTransformType.json,
    table = apiTableOut.uuid
  )

  val postTransformReq = new HttpPost(info.apiUrl.resolve(s"orgs/${project.org}/projects/${project.uuid}/tables/${apiTableOut.uuid}/transforms/")).also { post =>
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
