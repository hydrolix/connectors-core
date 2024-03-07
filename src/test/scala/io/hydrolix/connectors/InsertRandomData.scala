package io.hydrolix.connectors

import java.io.FileOutputStream

import com.fasterxml.jackson.databind.node.{IntNode, ObjectNode}
import org.apache.hc.client5.http.classic.methods.HttpPost
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.core5.http.ClassicHttpResponse
import org.apache.hc.core5.http.io.entity.{EntityUtils, StringEntity}

import io.hydrolix.connectors.JSON.ObjectNodeStuff
import io.hydrolix.connectors.api.HdxOutputColumn

object InsertRandomData {
  def main(args: Array[String]): Unit = {
    val info = HdxConnectionInfo.fromEnv()
    val dbName = args(0)
    val tableName = args(1)
    val transformName = args(2)

    val api = new HdxApiSession(info)

    val transform = api.transforms(dbName, tableName)
      .find(_.name == transformName)
      .getOrElse(sys.error(s"Transform $transformName doesn't exist"))

    val client = HttpClients.createDefault()

    val objs = DataGen(transform.settings.outputColumns, 100)

    val all = objs.map(node => JSON.objectMapper.writeValueAsString(node)).mkString("\n")

    val post = new HttpPost(info.apiUrl.resolve("/ingest/event"))
    api.addAuthToken(post)
    post.addHeader("Content-Type", "application/json")
    post.addHeader("x-hdx-table", tableName)
    post.addHeader("x-hdx-transform", transform.name)
    post.setEntity(new StringEntity(all))

    write(transform.settings.outputColumns, objs)

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

  private def write(outCols: List[HdxOutputColumn], objs: List[ObjectNode]): Unit = {
    val all = objs.map(node => JSON.objectMapper.writeValueAsString(node)).mkString("\n")
    new FileOutputStream("/home/alex/recs.json").also { os =>
      os.write(all.getBytes("UTF-8"))
      os.close()
    }

    val columnsByName = outCols.zipWithIndex.map { case (col, i) =>
      col.name -> i
    }.toMap

    val columnar = JSON.objectMapper.createObjectNode().also { out =>
      val cols = Array.fill(outCols.size)(JSON.objectMapper.createArrayNode())
      for (row <- objs) {
        for ((field, node) <- row.asMap) {
          val pos = columnsByName(field)
          cols(pos).add(node)
        }
      }

      val colsObj = JSON.objectMapper.createObjectNode().also { colsObj =>
        for ((colArray, i) <- cols.zipWithIndex) {
          val hcol = outCols(i)
          colsObj.replace(hcol.name, colArray)
        }
      }

      out.replace("rows", IntNode.valueOf(objs.size))
      out.replace("cols", colsObj)
    }

    new FileOutputStream("/home/alex/columnar.json").also { os =>
      JSON.objectMapper.writeValue(os, columnar)
      os.close()
    }
  }
}
