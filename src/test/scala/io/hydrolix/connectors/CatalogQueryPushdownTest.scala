package io.hydrolix.connectors

import java.time.Instant

import com.google.common.collect
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

class CatalogQueryPushdownTest {
  import PushdownFixture._

  private def check(qmin: Option[Instant],
                    qmax: Option[Instant],
          shardKeyHashes: Set[String] = Set())
                  (query: (Option[Instant], Option[Instant], Set[String]) => (List[HdxDbPartition], List[HdxDbPartition], List[HdxDbPartition]))
                        : Unit =
  {
    val (all, selected, notSelected) = query(qmin, qmax, shardKeyHashes)

    val targetRange = (qmin, qmax) match {
      case (Some(min), Some(max)) => Some(collect.Range.closed(min, max))
      case (Some(min), None)      => Some(collect.Range.atLeast(min))
      case (None,      Some(max)) => Some(collect.Range.atMost(max))
      case (None,      None)      => None
    }

    assertEquals(all.size, selected.size + notSelected.size)

    targetRange match {
      case Some(range) =>
        assertTrue(selected.forall(p => p.timeRange.isConnected(range)))
        assertTrue(notSelected.forall(p => !p.timeRange.isConnected(range)))
      case None =>
        assertTrue(selected.nonEmpty)
        assertTrue(notSelected.isEmpty)
    }
  }

  @Test
  def `lower bound only`(): Unit = {
    check(Some(second(180)), None)(queryUnsharded)
  }

  @Test
  def `upper bound only`(): Unit = {
    check(None, Some(second(180)))(queryUnsharded)
  }

  @Test
  def `get all partitions when no bounds`(): Unit = {
    check(None, None)(queryUnsharded)
  }

  @Test
  def `get all partitions when min & max bounds overlap everything`(): Unit = {
    check(Some(second(30)), Some(second(330)))(queryUnsharded)

    val (all, parts, notSelected) = queryUnsharded(Some(second(30)), Some(second(330)))

    assertEquals(unshardedPartitions.size, parts.size)
  }

  @Test
  def `get all partitions when min & max bounds exactly match partitions`(): Unit = {
    check(Some(second(0)), Some(second(360)))(queryUnsharded)

    val (all, parts, notSelected) = queryUnsharded(Some(second(0)), Some(second(360)))

    assertEquals(unshardedPartitions.size, parts.size)
  }

  @Test
  def `get all but two partitions when min & max bounds are just inside`(): Unit = {
    val (all, parts, notSelected) = queryUnsharded(Some(unshardedPartitions.head.maxTimestamp.plusSeconds(1)), Some(unshardedPartitions.last.minTimestamp.minusSeconds(1)))

    assertEquals(unshardedPartitions.size - 2, parts.size)
  }

  @Test
  def `get half of partitions when shardKey = "Alex"`(): Unit = {
    val (all, parts, notSelected) = querySharded(None, None, Set(WyHash("Alex")))
    assertEquals(shardedPartitions.size / 2, parts.size)
  }

  @Test
  def `get all partitions when shard key unspecified`(): Unit = {
    val (all, parts, notSelected) = querySharded(None, None, Set())
    assertEquals(shardedPartitions.size, parts.size)
  }

  @Test
  def `get correct partitions with min timestamp and shard keys`(): Unit = {
    val min = second(210)

    val (all, parts, notSelected) = querySharded(Some(min), None, Set(WyHash("Alex"), WyHash("Bob")))
    assertEquals(10, parts.size)
    val targetRange = collect.Range.atLeast(min)
    assertTrue(parts.forall(p => p.timeRange.isConnected(targetRange)))
  }

  @Test
  def `get correct partitions with max timestamp and shard keys`(): Unit = {
    val max = second(195)

    val (all, parts, notSelected) = querySharded(None, Some(max), Set(WyHash("Alex"), WyHash("Bob")))
    assertEquals(14, parts.size)
    val targetRange = collect.Range.atMost(max)
    assertTrue(parts.forall(p => p.timeRange.isConnected(targetRange)))
  }

  @Test
  def `get correct partitions with min & max timestamps and shard keys`(): Unit = {
    val min = second(90)
    val max = second(120)

    val (all, parts, notSelected) = querySharded(Some(min), Some(max), Set(WyHash("Alex"), WyHash("Bob")))
    assertEquals(8, parts.size)
    val targetRange = collect.Range.closed(min, max)
    assertTrue(parts.forall(p => p.timeRange.isConnected(targetRange)))
  }
}
