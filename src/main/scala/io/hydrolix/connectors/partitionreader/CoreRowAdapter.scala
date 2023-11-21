package io.hydrolix.connectors.partitionreader

import java.time.Instant
import scala.collection.JavaConverters._
import scala.collection.mutable

import com.fasterxml.jackson.databind.node.{BooleanNode, NumericNode, TextNode}

import io.hydrolix.connectors.expr.{ArrayLiteral, MapLiteral, StructLiteral}
import io.hydrolix.connectors.types._

object CoreRowAdapter extends RowAdapter[StructLiteral, ArrayLiteral[_], MapLiteral[_, _]] {
  type RB = CoreRowBuilder
  type AB = CoreArrayBuilder
  type MB = CoreMapBuilder

  override def newRowBuilder(`type`: StructType) = new CoreRowBuilder(`type`)
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
      case Int16Type => n.asInt().byteValue()
      case UInt16Type => n.asInt()
      case Int32Type => n.intValue()
      case Int64Type => n.longValue()
      case UInt32Type => n.longValue()
      case UInt64Type => n.decimalValue()
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

    override def build: StructLiteral = StructLiteral(values.toMap, `type`)
  }

  class CoreArrayBuilder(val `type`: ArrayType) extends ArrayBuilder {
    private val values = new java.util.ArrayList[Any]()

    override def set(pos: Int, value: Any): Unit = {
      values.ensureCapacity(pos+1) // this seems redundant with the next line, but it avoids repeated re-allocations
      while (values.size() < pos+1) values.add(null)
      values.set(pos, value)
    }

    override def setNull(pos: Int): Unit = ()

    override def build: ArrayLiteral[_] = ArrayLiteral(values.asScala.toList, `type`.elementType)
  }

  class CoreMapBuilder(val `type`: MapType) extends MapBuilder {
    private val values = mutable.HashMap[Any, Any]()

    override def put(key: Any, value: Any): Unit = values.put(key, value)

    override def putNull(key: Any): Unit = ()

    override def build: MapLiteral[_, _] = MapLiteral(values.toMap, `type`.keyType, `type`.valueType, `type`.valuesNullable)
  }
}
