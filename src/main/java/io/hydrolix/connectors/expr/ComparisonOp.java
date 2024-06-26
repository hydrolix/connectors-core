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

package io.hydrolix.connectors.expr;

import com.google.common.collect.ImmutableMap;

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

    public ComparisonOp negate() {
        return negated.get(this);
    }

    public static final Map<String, ComparisonOp> bySymbol =
        Arrays.stream(ComparisonOp.values())
              .collect(toUnmodifiableMap(ComparisonOp::getSymbol, identity()));

    private static final ImmutableMap<ComparisonOp, ComparisonOp> negated =
        ImmutableMap.of(
            EQ, NE,
            NE, EQ,
            GT, LE,
            GE, LT,
            LT, GE,
            LE, GT
        );
}
