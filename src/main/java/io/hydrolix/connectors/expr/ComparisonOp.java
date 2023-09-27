package io.hydrolix.connectors.expr;

import java.util.Arrays;
import java.util.Map;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toUnmodifiableMap;

public enum ComparisonOp {
    LT("<"),
    GT(">"),
    NE("<>"),
    EQ("="),
    GE(">="),
    LE("<="),
    ;

    private final String symbol;

    ComparisonOp(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }

    public static final Map<String, ComparisonOp> bySymbol =
            Arrays.stream(ComparisonOp.values())
                  .collect(toUnmodifiableMap(ComparisonOp::getSymbol, identity()));
}
