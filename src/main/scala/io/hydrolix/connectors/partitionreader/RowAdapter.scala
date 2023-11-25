package io.hydrolix.connectors.partitionreader

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._

import io.hydrolix.connectors.types._

abstract class RowAdapter[R, A, M] {
  type RB <: RowBuilder
  type AB <: ArrayBuilder
  type MB <: MapBuilder

  trait RowBuilder {
    val `type`: StructType
    def setNull(name: String): Unit
    def setField(name: String, value: Any): Unit
    def build: R
  }

  trait ArrayBuilder {
    val `type`: ArrayType
    def set(pos: Int, value: Any): Unit
    def setNull(pos: Int): Unit
    def build: A
  }

  trait MapBuilder {
    val `type`: MapType
    def put(key: Any, value: Any): Unit
    def putNull(key: Any): Unit
    def build: M
  }

  def row(rowId: Int, `type`: StructType, obj: ObjectNode): R = {
    val rowBuilder = newRowBuilder(`type`, rowId)

    `type`.fields.foreach { col =>
      val node = obj.get(col.name) // TODO can we be sure the names match exactly?
      val value = node2Any(node, col.`type`)
      rowBuilder.setField(col.name, value)
    }

    rowBuilder.build
  }

  def newRowBuilder(`type`: StructType, rowId: Int): RB

  def newArrayBuilder(`type`: ArrayType): AB

  def newMapBuilder(`type`: MapType): MB

  // TODO consider moving this JSON munging stuff to a different place

  /** Convert a JVM String into whatever the implementation expects, e.g. a UTF8String for Spark */
  def string(value: String): Any
  /** Convert a JSON TextNode into the expected `type` */
  def jsonString(value: TextNode, `type`: ValueType): Any
  /** Convert a JSON NumberNode into the expected `type` */
  def jsonNumber(n: NumericNode, `type`: ValueType): Any
  /** Convert a JSON BooleanNode into the expected `type` */
  def jsonBoolean(n: BooleanNode, `type`: ValueType): Any

  def node2Any(node: JsonNode, dt: ValueType): Any = {
    node match {
      case null => null
      case n if n.isNull => null
      case s: TextNode => jsonString(s, dt)
      case n: NumericNode => jsonNumber(n, dt)
      case b: BooleanNode => jsonBoolean(b, dt)
      case a: ArrayNode =>
        dt match {
          case at @ ArrayType(elementType, _) =>
            val arr = newArrayBuilder(at)
            for ((jv, i) <- a.asScala.zipWithIndex) {
              node2Any(jv, elementType) match {
                case null => arr.setNull(i)
                case other => arr.set(i, other)
              }
            }
            arr.build

          case other => sys.error(s"TODO JSON array needs conversion from $other to $dt")
        }
      case obj: ObjectNode =>
        dt match {
          case mt @ MapType(keyType, valueType, _) =>
            if (keyType != StringType) sys.error(s"TODO JSON map keys are $keyType, not strings")

            val m = newMapBuilder(mt)

            for (kv <- obj.fields().asScala) {
              val k = string(kv.getKey)
              kv.getValue match {
                case null => m.putNull(k)
                case node if node.isNull => m.putNull(k)
                case other => m.put(k, node2Any(other, valueType))
              }
            }
            m.build

          case st @ StructType(fields) =>
            val r = newRowBuilder(st, -1)

            for (kv <- obj.fields().asScala) {
              val name = kv.getKey

              kv.getValue match {
                case null => r.setNull(name)
                case node if node.isNull => r.setNull(name)
                case other => r.setField(name, node2Any(other, st.byName(name).`type`))
              }
            }
            r.build

          case other => sys.error(s"TODO JSON map needs conversion from $other to $dt")
        }
    }
  }
}
