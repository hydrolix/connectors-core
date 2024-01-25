package io.hydrolix.connectors

import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.types.{ArrayType, TimestampType}

class PartitionPruningTest {

  import PushdownFixture._

  @Test
  def `all partitions pruned when [timestamp == -1]`(): Unit = {
    val notPruned = prune(unshardedPartitions, Equal(getTimestamp, TimestampLiteral(second(-1))))

    assertEquals("Zero partitions include t=-1s", 0, notPruned.size)
  }

  @Test
  def `two partitions remain for [timestamp == 30]`(): Unit = {
    val notPruned = prune(unshardedPartitions, Equal(getTimestamp, TimestampLiteral(second(30))))

    assertEquals("Two partitions include t=30s", 2, notPruned.size)
  }

  @Test
  def `three partitions remain when [timestamp == 60]`(): Unit = {
    val notPruned = prune(unshardedPartitions, Equal(getTimestamp, TimestampLiteral(second(60))))

    assertEquals("Two partitions include t=60s", 3, notPruned.size)
  }

  @Test
  def `all partitions remain when [timestamp == 60 OR name='Alex']`(): Unit = {
    val notPruned = prune(unshardedPartitions, Or(Equal(getTimestamp, TimestampLiteral(second(60))), Equal(getName, StringLiteral("Alex"))))

    assertEquals("Nothing pruned when non-uniform top-level OR", unshardedPartitions.size, notPruned.size)
  }

  @Test
  def `five partitions remain when [timestamp = 60 OR timestamp = 300]`(): Unit = {
    val notPruned = prune(unshardedPartitions, Or(Equal(getTimestamp, TimestampLiteral(second(60))), Equal(getTimestamp, TimestampLiteral(second(300)))))

    assertEquals("5 partitions remain with uniform top-level OR", 5, notPruned.size)
  }

  @Test
  def `five partitions remain when [timestamp IN (60, 300)]`(): Unit = {
    val notPruned = prune(unshardedPartitions, In(getTimestamp, ArrayLiteral(List(second(60), second(300)), ArrayType(TimestampType.Millis))))

    assertEquals("5 partitions remain with top-level IN", 5, notPruned.size)
  }

  @Test
  def `five partitions remain when [timestamp BETWEEN 120 AND 180]`(): Unit = {
    val notPruned = prune(unshardedPartitions, Between(getTimestamp, TimestampLiteral(second(120)), TimestampLiteral(second(180))))

    assertEquals("5 partitions remain with top-level BETWEEN", 5, notPruned.size)
  }

  @Test
  def `five partitions remain when [timestamp IN (60) OR timestamp = 300]`(): Unit = {
    val notPruned = prune(unshardedPartitions, Or(
      In(getTimestamp, ArrayLiteral(List(second(60)), ArrayType(TimestampType.Millis))),
      Equal(getTimestamp, TimestampLiteral(second(300))))
    )

    assertEquals("5 partitions remain with [(timestamp IN (60)) OR (timestamp = 300)]", 5, notPruned.size)
  }
}
