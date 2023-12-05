package io.hydrolix.connectors

import java.io.FileOutputStream
import java.time.Instant

import org.apache.hc.client5.http.classic.methods.HttpPost
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.core5.http.ClassicHttpResponse
import org.apache.hc.core5.http.io.entity.{EntityUtils, StringEntity}

import io.hydrolix.connectors.TestUtils.{allColumns, connectionInfo}

object InsertRandomData extends App {
  val info = connectionInfo()
  val dbName = args(0)
  val tableName = args(1)
  val transformName = args(2)

  val api = new HdxApiSession(info)

  val project = api.database(dbName).getOrElse(sys.error(s"Database $dbName doesn't exist"))
  val table = api.table(dbName, tableName).getOrElse(sys.error(s"Table $dbName.$tableName doesn't exist"))
  val transform = api.transforms(dbName, tableName).find(_.name == transformName).getOrElse(sys.error(s"Transform $transformName doesn't exist"))
  val primaryCol = allColumns.find(_.datatype.primary).getOrElse(sys.error("Couldn't find primary column"))

  private val client = HttpClients.createDefault()

  val rng = new java.util.Random()
  val startTime = Instant.now()

  val objs = List.fill(100) {
    JSON.objectMapper.createObjectNode().also { obj =>
      for (hcol <- allColumns) {
        obj.replace(hcol.name, DataGen(hcol.datatype, !hcol.datatype.primary, rng))
      }
    }
  }

  val all = objs.map(node => JSON.objectMapper.writeValueAsString(node)).mkString("\n")
  new FileOutputStream("/home/alex/recs.json").also { os =>
    os.write(all.getBytes("UTF-8"))
    os.close()
  }

  val post = new HttpPost(info.apiUrl.resolve("/ingest/event"))
  api.addAuthToken(post)
  post.addHeader("Content-Type", "application/json")
  post.addHeader("x-hdx-table", tableName)
  post.addHeader("x-hdx-transform", transform.name)
  post.setEntity(new StringEntity(all))

  client.execute(post, { resp: ClassicHttpResponse =>
    val body = EntityUtils.toString(resp.getEntity)
    if (resp.getCode / 200 == 2) {
      println(s"Ingested ${objs.size} records")
    } else {
      println(s"Ingest failed with code ${resp.getCode}: ${resp.getReasonPhrase}")
      println(body)
    }
  })
}
