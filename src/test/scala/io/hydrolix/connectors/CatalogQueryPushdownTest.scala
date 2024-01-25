package io.hydrolix.connectors

import org.junit.Assert.assertEquals
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
}
