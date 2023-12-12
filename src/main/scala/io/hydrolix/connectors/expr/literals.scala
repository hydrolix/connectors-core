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

//noinspection TypeAnnotation
//noinspection ScalaUnusedSymbol
package io.hydrolix.connectors.expr

import java.time.Instant
import java.{math => jm}
import scala.collection.immutable.BitSet

import io.hydrolix.connectors.instantToMicros
import io.hydrolix.connectors.types._

trait Literal[+T] extends Expr[T] {
  val value: T
  val `type`: ValueType

  override val children = Nil
}
object Literal {
  def unapply[T](lit: Literal[T]): Option[(T, ValueType)] = {
    Some(lit.value, lit.`type`)
  }
}

case class BooleanLiteral private(value: Boolean) extends Literal[Boolean] {
  override val `type` = BooleanType
}
object BooleanLiteral {
  val True = BooleanLiteral(true)
  val False = BooleanLiteral(false)
}

case class StringLiteral(value: String) extends Literal[String] {
  override val `type` = StringType
}

case class Int8Literal(value: Byte) extends Literal[Byte] {
  override val `type` = Int8Type
}

case class Int16Literal(value: Short) extends Literal[Short] {
  override val `type` = Int16Type
}

case class Int32Literal(value: Int) extends Literal[Int] {
  override val `type` = Int32Type
}
object Int32Literal {
  val `0` = Int32Literal(0)
  val `1` = Int32Literal(1)
}

case class Int64Literal(value: Long) extends Literal[Long] {
  override val `type` = Int64Type
}
object Int64Literal {
  val `0l` = Int64Literal(0L)
  val `1l` = Int64Literal(1L)
}

case class UInt8Literal(value: Short) extends Literal[Short] {
  override val `type` = UInt8Type
}

case class UInt16Literal(value: Int) extends Literal[Int] {
  override val `type` = UInt16Type
}

case class UInt32Literal(value: Long) extends Literal[Long] {
  override val `type` = UInt32Type
}
object UInt32Literal {
  val `0u` = UInt32Literal(0)
  val `1u` = UInt32Literal(1)
}

case class UInt64Literal(value: jm.BigDecimal) extends Literal[jm.BigDecimal] {
  override val `type` = UInt64Type
}
object UInt64Literal {
  val `0lu` = UInt64Literal(jm.BigDecimal.ZERO)
  val `1lu` = UInt64Literal(jm.BigDecimal.ONE)
}

case class Float32Literal(value: Float) extends Literal[Float] {
  override val `type` = Float32Type
}
object Float32Literal {
  val `0f` = Float32Literal(0f)
  val `1f` = Float32Literal(1f)
}

case class Float64Literal(value: Double) extends Literal[Double] {
  override val `type` = Float64Type
}
object Float64Literal {
  val `0d` = Float64Literal(0d)
  val `1d` = Float64Literal(1d)
}

case class DecimalLiteral(value: jm.BigDecimal) extends Literal[jm.BigDecimal] {
  override val `type` = DecimalType(value.precision(), value.scale())
}

/**
 * Note that since this contains an Instant, it declares its type as TimestampType(6) but consumers of the expression
 * may want different precision.
 */
case class TimestampLiteral(value: Instant) extends Literal[Instant] {
  override val `type` = TimestampType(6)
}

case class ArrayLiteral[T](override val  value: Seq[T],
                           override val `type`: ArrayType,
                                         nulls: BitSet)
  extends Literal[Seq[T]]
{
  if (!`type`.elementsNullable && nulls.nonEmpty) sys.error("Null value(s) found in non-nullable array")

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append("[")

    val vs = value.zipWithIndex.map {
      case (v, i) => if (nulls(i)) "null" else v.toString
    }

    sb.append(vs.mkString(","))
    sb.append("]")
    sb.toString()
  }
}

case class Row(values: Seq[Any]) extends Serializable

object Row {
  val empty = Row(Nil)
}

case class StructLiteral(value: Row, `type`: StructType) extends Literal[Row] {
  private def check(pos: Int): Unit = {
    require(pos <= value.values.size - 1, s"Field index $pos out of range, must be ${value.values.size - 1}")
  }

  def getLong(pos: Int): Long = {
    check(pos)
    value.values(pos) match {
      case i: Instant => instantToMicros(i)
      case b: Byte => b.toLong
      case s: Short => s.toLong
      case i: Int => i.toLong
      case l: Long => l
      case bd: BigDecimal => bd.toLongExact
    }
  }

  def isNullAt(ordinal: Int): Boolean = {
    check(ordinal)
    value.values(ordinal) == null
  }

  def getBoolean(ordinal: Int): Boolean = {
    check(ordinal)
    value.values(ordinal) match {
      case b: Boolean => b
    }
  }

  def getByte(ordinal: Int): Byte = {
    check(ordinal)
    value.values(ordinal) match {
      case b: Byte => b
    }
  }

  def getShort(ordinal: Int): Short = {
    check(ordinal)
    value.values(ordinal) match {
      case b: Byte => b.toShort
      case s: Short => s
    }
  }

  def getInt(ordinal: Int): Int = {
    check(ordinal)
    value.values(ordinal) match {
      case b: Byte => b.toInt
      case s: Short => s.toInt
      case c: Char => c.toInt
      case i: Int => i
    }
  }

  def getFloat(ordinal: Int): Float = {
    check(ordinal)
    value.values(ordinal) match {
      case f: Float => f
    }
  }

  def getDouble(ordinal: Int): Double = {
    check(ordinal)
    value.values(ordinal) match {
      case f: Float => f
      case d: Double => d
    }
  }

  def getDecimal(ordinal: Int, precision: Int, scale: Int): BigDecimal = {
    check(ordinal)
    value.values(ordinal) match {
      case bd: BigDecimal => bd // TODO what should we do about the precision & scale args?
      case f: Float => BigDecimal(f.toDouble)
      case d: Double => BigDecimal(d)
      case b: Byte => BigDecimal(b)
      case s: Short => BigDecimal(s)
      case c: Char => BigDecimal(c)
      case i: Int => BigDecimal(i)
      case l: Long => BigDecimal(l)
    }
  }

  def getString(ordinal: Int): String = {
    check(ordinal)
    value.values(ordinal) match {
      case s: String => s
      case other => other.toString // TODO do we actually want this?
    }
  }
}
