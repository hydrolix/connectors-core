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

import java.time.Instant
import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.node.{BooleanNode, NumericNode, TextNode}

import io.hydrolix.connectors.expr.Row
import io.hydrolix.connectors.types._

object CoreRowAdapter extends RowAdapter[Row, Seq[AnyRef], Map[_, _]] {
  type RB = CoreRowBuilder
  type AB = CoreArrayBuilder
  type MB = CoreMapBuilder

  override def newRowBuilder(`type`: StructType, rowId: Int) = new CoreRowBuilder(`type`)
  override def newArrayBuilder(`type`: ArrayType) = new CoreArrayBuilder(`type`)
  override def newMapBuilder(`type`: MapType) = new CoreMapBuilder(`type`)

  /** Convert a JVM String into whatever the implementation expects, e.g. a UTF8String for Spark */
  override def string(value: String): Any = value

  /** Convert a JSON TextNode into the expected `type` */
  override def jsonString(value: TextNode, `type`: ValueType): Any = {
    `type` match {
      case StringType => value.textValue()
      case BooleanType => value.textValue().toLowerCase() == "true"
      case TimestampType(_) => Instant.parse(value.textValue())
    }
  }

  /** Convert a JSON NumberNode into the expected `type` */
  override def jsonNumber(n: NumericNode, `type`: ValueType): Any = {
    `type` match {
      case Float32Type => n.floatValue()
      case Float64Type => n.doubleValue()
      case Int8Type => n.asInt().byteValue()
      case UInt8Type => n.asInt().shortValue()
      case Int16Type => n.asInt().shortValue()
      case UInt16Type => n.asInt()
      case Int32Type => n.intValue()
      case Int64Type => n.longValue()
      case UInt32Type => n.longValue()
      case UInt64Type => n.bigIntegerValue()
      case DecimalType(_,_) => n.decimalValue()
      case t @ TimestampType(_) => t.fromJson(n).getOrElse(sys.error(s"Can't convert JSON number $n to $t"))
    }
  }

  /** Convert a JSON BooleanNode into the expected `type` */
  override def jsonBoolean(n: BooleanNode, `type`: ValueType): Any = {
    `type` match {
      case BooleanType => n.booleanValue()
      case StringType => if (n.booleanValue()) "true" else "false"
    }
  }

  class CoreRowBuilder(val `type`: StructType) extends RowBuilder {
    private val values = mutable.HashMap[String, Any]()

    override def setNull(name: String): Unit = ()

    override def setField(name: String, value: Any): Unit = values(name) = value

    override def build = {
      Row(`type`.fields.map { f =>
        values(f.name)
      })
    }
  }

  class CoreArrayBuilder(val `type`: ArrayType) extends ArrayBuilder {
    private val values = new java.util.ArrayList[AnyRef]()
    private val nulls = mutable.BitSet()

    override def set(pos: Int, value: Any): Unit = {
      values.ensureCapacity(pos+1) // this seems redundant with the next line, but it avoids repeated re-allocations
      while (values.size() < pos+1) values.add(null)
      values.set(pos, value.asInstanceOf[AnyRef])
    }

    override def setNull(pos: Int): Unit = nulls(pos) = true

    override def build = values.asScala.toSeq
  }

  class CoreMapBuilder(val `type`: MapType) extends MapBuilder {
    private val values = mutable.HashMap[Any, Any]()
    private val nulls = mutable.HashSet[Any]()

    override def put(key: Any, value: Any): Unit = values.put(key, value)

    override def putNull(key: Any): Unit = nulls += key

    override def build: Map[_, _] = {
      (values ++ nulls.map(_ -> null)).toMap
    }
  }
}
