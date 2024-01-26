package io.hydrolix.connectors

import com.google.common.collect
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

class CatalogQueryPushdownTest {
  import PushdownFixture._

  @Test
  def `get all partitions when no bounds`(): Unit = {
    val parts = queryUnsharded(None, None)

    assertEquals(unshardedPartitions.size, parts.size)
  }

  @Test
  def `get all partitions when min & max bounds overlap everything`(): Unit = {
    val parts = queryUnsharded(Some(second(30)), Some(second(330)))

    assertEquals(unshardedPartitions.size, parts.size)
  }

  @Test
  def `get all partitions when min & max bounds exactly match partitions`(): Unit = {
    val parts = queryUnsharded(Some(second(0)), Some(second(360)))

    assertEquals(unshardedPartitions.size, parts.size)
  }

  @Test
  def `get all but two partitions when min & max bounds are just inside`(): Unit = {
    val parts = queryUnsharded(Some(unshardedPartitions.head.maxTimestamp.plusSeconds(1)), Some(unshardedPartitions.last.minTimestamp.minusSeconds(1)))

    assertEquals(unshardedPartitions.size - 2, parts.size)
  }

  @Test
  def `get half of partitions when shardKey = "Alex"`(): Unit = {
    val parts = querySharded(None, None, Set(WyHash("Alex")))
    assertEquals(shardedPartitions.size / 2, parts.size)
  }

  @Test
  def `get all partitions when shard key unspecified`(): Unit = {
    val parts = querySharded(None, None, Set())
    assertEquals(shardedPartitions.size, parts.size)
  }

  @Test
  def `get correct partitions with min timestamp and shard keys`(): Unit = {
    val min = second(210)

    val parts = querySharded(Some(min), None, Set(WyHash("Alex"), WyHash("Bob")))
    assertEquals(10, parts.size)
    val targetRange = collect.Range.atLeast(min)
    assertTrue(parts.forall(p => p.timeRange.isConnected(targetRange)))
  }

  @Test
  def `get correct partitions with max timestamp and shard keys`(): Unit = {
    val max = second(195)

    val parts = querySharded(None, Some(max), Set(WyHash("Alex"), WyHash("Bob")))
    assertEquals(14, parts.size)
    val targetRange = collect.Range.atMost(max)
    assertTrue(parts.forall(p => p.timeRange.isConnected(targetRange)))
  }

  @Test
  def `get correct partitions with min & max timestamps and shard keys`(): Unit = {
    val min = second(90)
    val max = second(120)

    val parts = querySharded(Some(min), Some(max), Set(WyHash("Alex"), WyHash("Bob")))
    assertEquals(8, parts.size)
    val targetRange = collect.Range.closed(min, max)
    assertTrue(parts.forall(p => p.timeRange.isConnected(targetRange)))
  }
}
