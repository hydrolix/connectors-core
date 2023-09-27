package io.hydrolix.connectors.expr

import org.junit.Assert.assertEquals
import org.junit.Test

class ComparisonOpTest {
  @Test
  def checkSymbols(): Unit = {
    val ops = ComparisonOp.values()
    val syms = ComparisonOp.bySymbol

    assertEquals("equal number of ops and symbols", ops.size, syms.size)

    for (op <- ops) {
      assertEquals("bySymbol points to the expected op", op, syms.get(op.getSymbol))
    }
  }
}
