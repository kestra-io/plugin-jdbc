package com.rockset.jdbc;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import io.kestra.core.utils.Rethrow;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class RocksetArrayOverride {
    public static Object getArray(Object array) throws IOException {
        RocksetArray rocksetArray = (RocksetArray) array;

        JsonNode[] jsonNodes = (JsonNode[]) rocksetArray.getArray();

        return Arrays
            .stream(jsonNodes)
            .map(Rethrow.throwFunction(jsonNode -> {
                if (jsonNode instanceof DoubleNode) {
                    return jsonNode.doubleValue();
                } else if (jsonNode instanceof BooleanNode) {
                    return jsonNode.booleanValue();
                } else if (jsonNode instanceof TextNode) {
                    return jsonNode.textValue();
                } else if (jsonNode instanceof BinaryNode) {
                    return jsonNode.binaryValue();
                } else if (jsonNode instanceof FloatNode) {
                    return jsonNode.floatValue();
                } else if (jsonNode instanceof LongNode) {
                    return jsonNode.longValue();
                } else if (jsonNode instanceof BigIntegerNode) {
                    return jsonNode.bigIntegerValue();
                } else if (jsonNode instanceof DecimalNode) {
                    return jsonNode.decimalValue();
                } else if (jsonNode instanceof IntNode) {
                    return jsonNode.intValue();
                } else if (jsonNode instanceof ShortNode) {
                    return jsonNode.shortValue();
                } else if (jsonNode instanceof NullNode) {
                    return null;
                }

                return jsonNode;
            }))
            .collect(Collectors.toList());
    }
}
