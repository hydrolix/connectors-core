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
import scala.collection.mutable

import com.typesafe.scalalogging.Logger

import io.hydrolix.connectors.HdxConnectionInfo._
import io.hydrolix.connectors.api.HdxStorageSettings
import io.hydrolix.connectors.types.{StructField, StructType}

final class HdxTableCatalog {
  private val log = Logger(getClass)

  var name: String = _
  private var info: HdxConnectionInfo = _
  private var api: HdxApiSession = _
  private var jdbc: HdxJdbcSession = _
  private var storageSettings: Map[UUID, HdxStorageSettings] = _
  private var queryMode: HdxQueryMode = _

  private val columnsCache = mutable.HashMap[(String, String), List[HdxColumnInfo]]()

  protected def columns(db: String, table: String): List[HdxColumnInfo] = {
    columnsCache.getOrElseUpdate((db, table), {
      val view = api.defaultView(db, table)

      view.settings.outputColumns.map { col =>
        val stype = Types.hdxToValueType(col.datatype)

        HdxColumnInfo(
          col.name,
          col.datatype,
          nullable = true,
          stype,
          if (col.datatype.index) 2 else 0 // TODO this will sometimes be wrong if the column wasn't always indexed
        )
      }
    })
  }

  def initialize(name: String, opts: Map[String, String]): Unit = {
    this.name = name
    this.info = HdxConnectionInfo.fromOpts(opts)
    this.api = new HdxApiSession(info)
    this.jdbc = HdxJdbcSession(info)
    this.queryMode = opt(opts, OPT_QUERY_MODE).map(HdxQueryMode.of).getOrElse(HdxQueryMode.AUTO)

    val bn = HdxConnectionInfo.opt(opts, OPT_STORAGE_BUCKET_NAME)
    val bp = HdxConnectionInfo.opt(opts, OPT_STORAGE_BUCKET_PATH)
    val r = HdxConnectionInfo.opt(opts, OPT_STORAGE_REGION)
    val c = HdxConnectionInfo.opt(opts, OPT_STORAGE_CLOUD)
    val e = HdxConnectionInfo.opt(opts, OPT_STORAGE_ENDPOINT_URI)

    // TODO this is ugly
    if ((bn ++ bp ++ r ++ c).size == 4) {
      this.storageSettings = Map(uuid0 -> HdxStorageSettings(true, bn.get, bp.get, r.get, c.get, e))
    } else {
      val storages = api.storages()
      if (storages.isEmpty) {
        sys.error("No storages available from API, and no storage settings provided in configuration")
      } else {
        val storages = api.storages().map(storage => storage.uuid -> storage.settings).toMap
        if (storages.isEmpty) {
          sys.error("No storages available from API, and no storage settings provided in configuration")
        } else if (e.isEmpty) {
          this.storageSettings = storages
        } else {
          log.info(s"Using endpoint override ${e.get}")
          this.storageSettings = storages.map { case (id, storage) => id -> storage.copy(endpoint = e)}
        }
      }
    }
  }

  private def inferSchema(options: Map[String, String]): StructType = {
    val db = options.getOrElse(OPT_PROJECT_NAME, sys.error(s"${OPT_PROJECT_NAME}is required"))
    val table = options.getOrElse(OPT_TABLE_NAME, sys.error(s"${OPT_TABLE_NAME}is required"))

    val cols = columns(db, table)

    StructType(cols.map { col =>
      StructField(col.name, col.`type`, false) // TODO nullability?
    })
  }

  private def getTable(schema: StructType, properties: Map[String, String]): HdxTable = {
    val db = properties.getOrElse(OPT_PROJECT_NAME, sys.error(s"${OPT_PROJECT_NAME}is required"))
    val table = properties.getOrElse(OPT_TABLE_NAME, sys.error(s"${OPT_TABLE_NAME}is required"))

    val apiTable = api.table(db, table)
      .getOrElse(throw NoSuchTableException(db, table))
    val primaryKey = api.pk(db, table)

    HdxTable(
      info,
      storageSettings,
      List(db, table),
      schema,
      primaryKey.name,
      apiTable.settings.shardKey,
      apiTable.settings.sortKeys,
      columns(db, table).map(col => col.name -> col).toMap,
      queryMode
    )
  }

  def listTables(namespace: List[String]): List[List[String]] = {
    assert(namespace.length == 1, "Namespace paths must have exactly one element (DB name)")
    api.tables(namespace.head).map { ht =>
      namespace :+ ht.name
    }
  }

  def loadTable(ident: List[String]): HdxTable = {
    assert(ident.length == 2, "Namespace paths must have exactly two elements (DB name and table name)")
    val List(db, name) = ident

    val opts = info.asMap +
      (OPT_PROJECT_NAME -> db) +
      (OPT_TABLE_NAME -> name)

    val schema = inferSchema(opts)

    getTable(
      schema,
      Map(
        OPT_PROJECT_NAME -> db,
        OPT_TABLE_NAME -> name
      )
    )
  }

  def listNamespaces(): List[List[String]] = {
    for {
      db <- api.databases()
      table <- api.tables(db.name)
    } yield List(db.name, table.name)
  }
}
