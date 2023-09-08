package io.hydrolix.connectors.partitionreader

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.sys.error
import scala.util.Using
import scala.util.control.Breaks.{break, breakable}

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, BooleanNode, NumericNode, ObjectNode, TextNode}
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.types.{ArrayType, DecimalType, MapType, StringType, StructType, TimestampType, ValueType}
import io.hydrolix.connectors.{HdxConnectionInfo, HdxPartitionScanPlan, HdxStorageSettings, JSON, RowAdapter}

abstract class RowParser[T](schema: StructType) {
  def apply(line: String): T
}

final class RowPartitionReader[T <: AnyRef](         val           info: HdxConnectionInfo,
                                                     val        storage: HdxStorageSettings,
                                                     val primaryKeyName: String,
                                                     val           scan: HdxPartitionScanPlan,
                                                     val          parse: RowParser[T],
                                            override val     doneSignal: T)
  extends HdxPartitionReader[T]
{
  override def outputFormat = "json"

  override def handleStdout(stdout: InputStream): Unit = {
    Using.Manager { use =>
      val reader = use(new BufferedReader(new InputStreamReader(stdout)))
      breakable {
        while (true) {
          val line = reader.readLine()
          if (line == null) {
            stdoutQueue.put(doneSignal)
            break()
          } else {
            expectedLines.incrementAndGet()
            stdoutQueue.put(parse(line))
          }
        }
      }
    }.get
  }
}

/*
final class HdxReaderRowJson[R, A, M](val adapter: RowAdapter[R,A,M]) {
  private val log = LoggerFactory.getLogger(getClass)

  def apply(schema: StructType, jsonLine: String): R = {
    val obj = JSON.objectMapper.readValue[ObjectNode](jsonLine)

    val row = adapter.create(schema)

    val values = schema.fields.map { col =>
      val node = obj.get(col.name) // TODO can we be sure the names match exactly?
      val value = node2Any(node, col.name, col.`type`)
      adapter.setField(row, col.name, value)
    }

    row
  }

  private def node2Any(node: JsonNode, name: String, dt: ValueType): Any = {
    node match {
      case null => null
      case n if n.isNull => null
      case s: TextNode => str(s, dt)
      case n: NumericNode => num(n, dt)
      case b: BooleanNode => bool(b, dt)
      case a: ArrayNode =>
        dt match {
          case ArrayType(elementType, _) =>
            val values = a.asScala.map(node2Any(_, name, elementType)).toList
            new GenericArrayData(values)

          case other => error(s"TODO JSON array field $name needs conversion from $other to $dt")
        }
      case obj: ObjectNode =>
        dt match {
          case MapType(keyType, valueType, _) =>
            if (keyType != DataTypes.StringType) error(s"TODO JSON map field $name keys are $keyType, not strings")
            val keys = obj.fieldNames().asScala.map(UTF8String.fromString).toArray
            val values = obj.fields().asScala.map(entry => node2Any(entry.getValue, name, valueType)).toArray
            new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))

          case other => error(s"TODO JSON map field $name needs conversion from $other to $dt")
        }
    }
  }

  private def str(s: TextNode, dt: ValueType): Any = {
    dt match {
      case StringType => UTF8String.fromString(s.textValue())
      case TimestampType(precision) =>
        val inst = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(s.textValue()))
        DateTimeUtils.instantToMicros(inst)
      case other => error(s"TODO make a $other from string value '$s'")
    }
  }

  private def num(n: NumericNode, dt: ValueType): Any = {
    dt match {
      case DecimalType(_, _) => n.decimalValue() // TODO do we care to preserve the precision and scale here?
      case LongType => n.longValue()
      case DoubleType => n.doubleValue()
      case FloatType => n.floatValue()
      case IntegerType => n.intValue()
      case ShortType => n.shortValue()
      case other => error(s"TODO make a $other from JSON value '$n'")
    }
  }


  private def bool(n: BooleanNode, dt: DataType) = {
    dt match {
      case BooleanType => n.booleanValue()
      case IntegerType => if (n.booleanValue()) 1 else 0
      case LongType => if (n.booleanValue()) 1L else 0L
      case DoubleType => if (n.booleanValue()) 1.0 else 0.0
      case other => error(s"TODO make a $other from JSON value '$n'")
    }
  }
}*/
