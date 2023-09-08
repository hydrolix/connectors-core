package io.hydrolix.connectors

import java.time.Instant

import io.hydrolix.connectors.types.{ArrayType, MapType, StructType, ValueType}

trait ArrayBuilder[A, E] {
  val `type`: ArrayType
  def setNull(pos: Int)
  def set(pos: Int, value: E)
  def get: A
}

trait MapBuilder[M, K, V] {
  val `type`: MapType
  def putNull(key: K)
  def put(key: K, value: V)
  def get: M
}

abstract class RowAdapter[R, A, M]() {
  type AA <: ArrayBuilder[A, _]
  type MA <: MapBuilder[M, _, _]

  def create(schema: StructType): R

  def newArrayBuilder(`type`: ArrayType): AA
  def newMapBuilder(`type`: MapType): MA

  def setNull(name: String)
  def setByte(name: String, value: Byte)
  def setInt(name: String, value: Int)
  def setLong(name: String, value: Long)
  def setFloat(name: String, value: Float)
  def setDouble(name: String, value: Double)
  def setBoolean(name: String, value: Boolean)
  def setString(name: String, value: String)
  def setTimestamp(name: String, value: Instant)
  def setDecimal(name: String, value: BigDecimal)
  def setArray(name: String, value: A)
  def setMap(name: String, value: M)

  def get: R
}
