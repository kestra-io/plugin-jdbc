package io.kestra.plugin.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Collects per-column statistics row by row, from the rows already read for the task output (no second query).
 * Collection stops after {@code sampleRows} rows; the caller keeps storing rows.
 */
public final class ColumnStatsCollector {
    static final int MAX_MIN_MAX_STRING_LENGTH = 256;
    private static final int MEDIAN_RESERVOIR_SIZE = 10_000;
    private static final Set<Integer> SKIPPED_TYPES = Set.of(
        Types.BLOB, Types.CLOB, Types.NCLOB, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY,
        Types.ARRAY, Types.STRUCT, Types.SQLXML, Types.JAVA_OBJECT, Types.REF, Types.DATALINK
    );

    private final long sampleRows;
    private final Map<String, ColumnAccumulator> columns = new LinkedHashMap<>();
    private final Map<String, String> jdbcTypes = new LinkedHashMap<>();
    private long seenRows;

    public ColumnStatsCollector(long sampleRows) {
        this.sampleRows = sampleRows;
    }

    public void describe(ResultSetMetaData metaData) throws SQLException {
        for (var i = 1; i <= metaData.getColumnCount(); i++) {
            var label = metaData.getColumnLabel(i);
            var type = metaData.getColumnType(i);
            if (SKIPPED_TYPES.contains(type)) {
                jdbcTypes.put(label, null);
                continue;
            }
            jdbcTypes.put(label, jdbcTypeName(type));
            columns.computeIfAbsent(label, ColumnAccumulator::new);
        }
    }

    public void accept(Map<String, Object> row) {
        if (seenRows >= sampleRows) {
            return;
        }
        seenRows++;
        row.forEach((label, value) -> {
            if (jdbcTypes.containsKey(label) && jdbcTypes.get(label) == null) {
                return;
            }
            columns.computeIfAbsent(label, ColumnAccumulator::new).add(value);
        });
    }

    public List<Map<String, Object>> results() {
        var results = new ArrayList<Map<String, Object>>();
        columns.forEach((label, accumulator) -> results.add(accumulator.toMap(jdbcTypes.get(label))));
        return results;
    }

    private static String jdbcTypeName(int type) {
        for (var field : Types.class.getFields()) {
            try {
                if (field.getType() == int.class && field.getInt(null) == type) {
                    return field.getName();
                }
            } catch (IllegalAccessException ignored) {
                // Constants of java.sql.Types are public, so this cannot happen.
            }
        }
        return String.valueOf(type);
    }

    private static final class ColumnAccumulator {
        private final String column;
        private final HyperLogLog distinct = new HyperLogLog();
        private final List<Comparable<Object>> reservoir = new ArrayList<>();
        private final Random random = new Random(42);
        private long nulls;
        private long nonNulls;
        private Comparable<Object> min;
        private Comparable<Object> max;
        private boolean orderingBroken;
        private boolean longString;
        private double sum;
        private long numericCount;
        private Integer minLen;
        private Integer maxLen;

        private ColumnAccumulator(String column) {
            this.column = column;
        }

        @SuppressWarnings("unchecked")
        void add(Object value) {
            if (value == null) {
                nulls++;
                return;
            }
            nonNulls++;
            distinct.add(value.toString());

            if (value instanceof CharSequence text) {
                var length = text.length();
                minLen = minLen == null ? length : Math.min(minLen, length);
                maxLen = maxLen == null ? length : Math.max(maxLen, length);
                if (length > MAX_MIN_MAX_STRING_LENGTH) {
                    longString = true;
                    min = null;
                    max = null;
                }
            }
            if (value instanceof Number number) {
                sum += number.doubleValue();
                numericCount++;
            }
            var nonFinite = (value instanceof Double d && !Double.isFinite(d)) || (value instanceof Float f && !Float.isFinite(f));
            if (orderingBroken || longString || nonFinite || !(value instanceof Comparable<?>)) {
                return;
            }

            var comparable = (Comparable<Object>) normalize(value);
            try {
                if (min == null || comparable.compareTo(min) < 0) {
                    min = comparable;
                }
                if (max == null || comparable.compareTo(max) > 0) {
                    max = comparable;
                }
                if (value instanceof Number || value instanceof TemporalAccessor || value instanceof Date) {
                    sample(comparable);
                }
            } catch (ClassCastException e) {
                orderingBroken = true;
                min = null;
                max = null;
                reservoir.clear();
            }
        }

        private static Object normalize(Object value) {
            return value instanceof Number number && !(value instanceof BigDecimal)
                ? new BigDecimal(number.toString())
                : value;
        }

        private void sample(Comparable<Object> value) {
            if (reservoir.size() < MEDIAN_RESERVOIR_SIZE) {
                reservoir.add(value);
                return;
            }
            var slot = random.nextLong(nonNulls);
            if (slot < MEDIAN_RESERVOIR_SIZE) {
                reservoir.set((int) slot, value);
            }
        }

        Map<String, Object> toMap(String jdbcType) {
            var result = new LinkedHashMap<String, Object>();
            result.put("column", column);
            result.put("jdbcType", jdbcType);
            result.put("nulls", nulls);
            result.put("distinct", distinct.estimate());
            result.put("min", longString ? null : printable(min));
            result.put("max", longString ? null : printable(max));
            result.put("mean", numericCount > 0 && numericCount == nonNulls ? sum / numericCount : null);
            result.put("median", median());
            result.put("minLen", minLen);
            result.put("maxLen", maxLen);
            return result;
        }

        private Object median() {
            if (reservoir.isEmpty() || longString || orderingBroken) {
                return null;
            }
            var sorted = new ArrayList<>(reservoir);
            Collections.sort(sorted);
            return printable(sorted.get(sorted.size() / 2));
        }

        // ION-friendly value: numbers stay numeric, everything else is rendered as text.
        private static Object printable(Object value) {
            return switch (value) {
                case null -> null;
                case BigDecimal decimal -> decimal.stripTrailingZeros().scale() <= 0 && decimal.toBigInteger().bitLength() < 63
                    ? (Object) decimal.toBigInteger().longValue()
                    : (Object) decimal.doubleValue();
                case Number number -> number;
                default -> value.toString();
            };
        }
    }

    /** Small HyperLogLog (precision 12, about 1.6% error) so distinct counts stay approximate and bounded in memory. */
    static final class HyperLogLog {
        private static final int PRECISION = 12;
        private static final int REGISTERS = 1 << PRECISION;
        private final byte[] registers = new byte[REGISTERS];

        void add(String value) {
            var hash = mix(value);
            var index = (int) (hash >>> (64 - PRECISION));
            var rank = (byte) (Long.numberOfLeadingZeros((hash << PRECISION) | (1L << (PRECISION - 1))) + 1);
            if (rank > registers[index]) {
                registers[index] = rank;
            }
        }

        long estimate() {
            var sum = 0.0;
            var zeros = 0;
            for (var register : registers) {
                sum += 1.0 / (1L << register);
                if (register == 0) {
                    zeros++;
                }
            }
            var alpha = 0.7213 / (1 + 1.079 / REGISTERS);
            var estimate = alpha * REGISTERS * REGISTERS / sum;
            if (estimate <= 2.5 * REGISTERS && zeros > 0) {
                estimate = REGISTERS * Math.log((double) REGISTERS / zeros);
            }
            return Math.round(estimate);
        }

        // FNV-1a followed by the murmur3 finalizer, which spreads the bits the register index relies on.
        private static long mix(String value) {
            var hash = 0xcbf29ce484222325L;
            for (var i = 0; i < value.length(); i++) {
                hash ^= value.charAt(i);
                hash *= 0x100000001b3L;
            }
            hash ^= hash >>> 33;
            hash *= 0xff51afd7ed558ccdL;
            hash ^= hash >>> 33;
            hash *= 0xc4ceb9fe1a85ec53L;
            hash ^= hash >>> 33;
            return hash;
        }
    }
}
