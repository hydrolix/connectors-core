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

package io.hydrolix.connectors;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;

public enum HdxValueType {
    // Note: These are mixed-case because `boolean` and `double` are reserved words.
    // the enum constant name must match the `hdxName` when lower-cased!
    Boolean("boolean", true),
    Double("double", true),
    Int8("int8", true),
    Int32("int32", true),
    Int64("int64", true),
    String("string", true),
    UInt8("uint8", true),
    UInt32("uint32", true),
    UInt64("uint64", true),
    DateTime("datetime", true),
    DateTime64("datetime64", true),
    Epoch("epoch", true),
    Array("array", false),
    Map("map", false),
    ;

    private static final java.util.Map<String, HdxValueType> byName = new HashMap<>();
    static {
        for (HdxValueType vt : HdxValueType.values()) {
            byName.put(vt.getHdxName(), vt);
        }
    }

    HdxValueType(String name, boolean scalar) {
        this.hdxName = name;
        this.scalar = scalar;
    }

    private final String hdxName;
    private final boolean scalar;

    @JsonValue
    public java.lang.String getHdxName() {
        return hdxName;
    }

    public boolean isScalar() { return scalar; }

    public static HdxValueType forName(String s) {
        if (!byName.containsKey(s)) throw new IllegalArgumentException("No enum value for " + s);
        return byName.get(s);
    }
}

