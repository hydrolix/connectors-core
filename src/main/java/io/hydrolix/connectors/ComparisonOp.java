package io.hydrolix.connectors;

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
}
