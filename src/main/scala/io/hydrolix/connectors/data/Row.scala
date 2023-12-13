package io.hydrolix.connectors.data

import java.time.Instant
import java.{math => jm}

import io.hydrolix.connectors.instantToMicros

case class Row(values: Seq[Any]) extends Serializable {
  private def check(pos: Int): Unit = {
    require(pos <= values.size - 1, s"Field index $pos out of range, must be ${values.size - 1}")
  }

  def getLong(pos: Int): Long = {
    check(pos)
    values(pos) match {
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
    values(ordinal) == null
  }

  def getBoolean(ordinal: Int): Boolean = {
    check(ordinal)
    values(ordinal) match {
      case b: Boolean => b
    }
  }

  def getByte(ordinal: Int): Byte = {
    check(ordinal)
    values(ordinal) match {
      case b: Byte => b
    }
  }

  def getShort(ordinal: Int): Short = {
    check(ordinal)
    values(ordinal) match {
      case b: Byte => b.toShort
      case s: Short => s
    }
  }

  def getInt(ordinal: Int): Int = {
    check(ordinal)
    values(ordinal) match {
      case b: Byte => b.toInt
      case s: Short => s.toInt
      case c: Char => c.toInt
      case i: Int => i
    }
  }

  def getFloat(ordinal: Int): Float = {
    check(ordinal)
    values(ordinal) match {
      case f: Float => f
    }
  }

  def getDouble(ordinal: Int): Double = {
    check(ordinal)
    values(ordinal) match {
      case f: Float => f
      case d: Double => d
    }
  }

  def getDecimal(ordinal: Int, precision: Int, scale: Int): jm.BigDecimal = {
    check(ordinal)
    values(ordinal) match {
      case bd: jm.BigDecimal => bd // TODO what should we do about the precision & scale args?
      case f: Float => jm.BigDecimal.valueOf(f.toDouble)
      case d: Double => jm.BigDecimal.valueOf(d)
      case b: Byte => jm.BigDecimal.valueOf(b)
      case s: Short => jm.BigDecimal.valueOf(s)
      case c: Char => jm.BigDecimal.valueOf(c)
      case i: Int => jm.BigDecimal.valueOf(i)
      case l: Long => jm.BigDecimal.valueOf(l)
    }
  }

  def getString(ordinal: Int): String = {
    check(ordinal)
    values(ordinal) match {
      case s: String => s
      case other => other.toString // TODO do we actually want this?
    }
  }
}

object Row {
  val empty = Row(Nil)
}
