package io.kestra.plugin.jdbc.clickhouse;

import java.util.Locale;

/**
 * Maps ClickHouse/chDB {@code outputFormat} values to Kestra storage file extensions
 * and decides when results should be normalized to Amazon Ion.
 *
 * <p>Maintainer guidance for issue #481:
 * <ul>
 *   <li>No format / Pretty* formats → Ion (tabular intermediate storage)</li>
 *   <li>Named file formats (CSVWithNames, Parquet, …) → dedicated extension</li>
 * </ul>
 */
final class ChDBOutputFormats {
    static final String DEFAULT_ION_EXTENSION = ".ion";
    static final String INTERNAL_TABULAR_FORMAT = "JSONEachRow";

    private ChDBOutputFormats() {
    }

    /**
     * @return {@code true} when the result should be stored as Amazon Ion
     *     (default / Pretty* display formats).
     */
    static boolean shouldStoreAsIon(String outputFormat) {
        if (outputFormat == null || outputFormat.isBlank()) {
            return true;
        }
        return outputFormat.trim().regionMatches(true, 0, "Pretty", 0, "Pretty".length());
    }

    /**
     * ClickHouse format used when executing the query.
     * Pretty / default paths use {@link #INTERNAL_TABULAR_FORMAT} so rows can be converted to Ion.
     */
    static String effectiveClickHouseFormat(String outputFormat) {
        if (shouldStoreAsIon(outputFormat)) {
            return INTERNAL_TABULAR_FORMAT;
        }
        return outputFormat.trim();
    }

    /**
     * File extension for the artifact uploaded to Kestra internal storage.
     */
    static String fileExtension(String outputFormat) {
        if (shouldStoreAsIon(outputFormat)) {
            return DEFAULT_ION_EXTENSION;
        }

        String format = outputFormat.trim().toLowerCase(Locale.ROOT);

        if (format.contains("parquet")) {
            return ".parquet";
        }
        if (format.contains("orc")) {
            return ".orc";
        }
        if (format.contains("avro")) {
            return ".avro";
        }
        if (format.contains("arrow")) {
            return ".arrow";
        }
        if (format.contains("protobuf")) {
            return ".pb";
        }
        if (format.contains("msgpack")) {
            return ".msgpack";
        }
        if (format.contains("xml")) {
            return ".xml";
        }
        if (format.contains("csv")) {
            return ".csv";
        }
        if (format.contains("tsv") || format.contains("tabseparated") || format.contains("tabseparatedraw")) {
            return ".tsv";
        }
        if (format.contains("json")) {
            return ".json";
        }
        if (format.contains("yaml") || format.contains("yml")) {
            return ".yaml";
        }
        if (format.contains("bson")) {
            return ".bson";
        }
        if (format.contains("native")) {
            return ".bin";
        }
        if (format.contains("rawblob") || format.equals("raw")) {
            return ".bin";
        }
        if (format.contains("lineasstring")) {
            return ".txt";
        }
        if (format.contains("markdown")) {
            return ".md";
        }
        if (format.contains("html")) {
            return ".html";
        }

        // Fall back to a sanitized format name so the artifact remains identifiable.
        String sanitized = format.replaceAll("[^a-z0-9]+", "_");
        return "." + (sanitized.isBlank() ? "out" : sanitized);
    }

    static String outputFileName(String outputFormat) {
        return "result" + fileExtension(outputFormat);
    }

    /**
     * ClickHouse format names are alphanumeric. Reject anything else so values
     * interpolated into {@code /bin/sh -c} cannot break out of the intended command.
     */
    static String requireSafeFormat(String format) {
        if (format == null || !format.matches("[A-Za-z0-9]+")) {
            throw new IllegalArgumentException("Invalid outputFormat: '" + format + "'");
        }
        return format;
    }
}
