package io.kestra.plugin.jdbc;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.jdbc.SqlTableExtractor.Confidence;
import io.kestra.plugin.jdbc.SqlTableExtractor.Dialect;
import io.kestra.plugin.jdbc.SqlTableExtractor.TableReference;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Helpers shared by the JDBC tasks to build the post-execution outputs.
 */
final class JdbcExecutionSupport {
    static final int MAX_TABLES = 50;
    static final int MAX_STATEMENT_BYTES = 64 * 1024;
    private static final Pattern SECRET_CALL = Pattern.compile("secret\\(\\s*['\"]([^'\"]+)['\"]\\s*\\)");

    private JdbcExecutionSupport() {
    }

    record TableSummary(List<TableReference> tables, boolean truncated) {
    }

    record Details(Long rowsAffected, Long bytes, Long durationMs, Integer warnings, TableSummary tables, String statement, URI columnStats) {
    }

    static Details details(
        RunContext runContext,
        TableSummary tables,
        String statement,
        Long rowsAffected,
        Long bytes,
        long durationNanos,
        int warnings,
        URI columnStats,
        String... tags
    ) {
        var duration = Duration.ofNanos(durationNanos);
        runContext.metric(Timer.of("duration", duration, tags));
        if (rowsAffected != null) {
            runContext.metric(Counter.of("rows.affected", rowsAffected, tags));
        }
        if (bytes != null) {
            runContext.metric(Counter.of("bytes", bytes, tags));
        }
        return new Details(rowsAffected, bytes, duration.toMillis(), warnings, tables, statement, columnStats);
    }

    static URI storeIon(RunContext runContext, List<Map<String, Object>> rows) throws IOException {
        var file = runContext.workingDir().createTempFile(".ion");
        try (var output = new BufferedOutputStream(Files.newOutputStream(file), FileSerde.BUFFER_SIZE)) {
            for (var row : rows) {
                FileSerde.write(output, row);
            }
        }
        return runContext.storage().putFile(file.toFile());
    }

    // JDBC returns -1 when the current result is a ResultSet or when there are no more results.
    static Long updateCount(Statement statement) throws SQLException {
        long count;
        try {
            count = statement.getLargeUpdateCount();
        } catch (SQLFeatureNotSupportedException | UnsupportedOperationException | AbstractMethodError e) {
            count = statement.getUpdateCount();
        }
        return count < 0 ? null : count;
    }

    static int countWarnings(Statement statement) {
        try {
            var count = 0;
            for (SQLWarning warning = statement.getWarnings(); warning != null; warning = warning.getNextWarning()) {
                count++;
            }
            return count;
        } catch (SQLException | RuntimeException e) {
            return 0;
        }
    }

    // Secrets are resolved from the `secret()` calls of the unrendered template, then masked in the rendered SQL.
    static String maskedStatement(RunContext runContext, Property<String> template, String rendered) throws IllegalVariableEvaluationException {
        if (rendered == null) {
            return null;
        }
        var masked = rendered;
        if (template != null) {
            var matcher = SECRET_CALL.matcher(String.valueOf(template));
            var seen = new HashSet<String>();
            while (matcher.find()) {
                if (!seen.add(matcher.group(1))) {
                    continue;
                }
                var value = runContext.render("{{ secret('" + matcher.group(1).replace("'", "") + "') }}");
                if (value != null && !value.isEmpty()) {
                    masked = masked.replace(value, "***");
                }
            }
        }
        return truncate(masked);
    }

    static String truncate(String statement) {
        var bytes = statement.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_STATEMENT_BYTES) {
            return statement;
        }
        return new String(bytes, 0, MAX_STATEMENT_BYTES, StandardCharsets.UTF_8);
    }

    static TableSummary tables(Connection connection, Collection<String> statements) {
        var dialect = connection == null ? Dialect.ANSI : dialectOf(connection);
        var unique = new ArrayList<TableReference>();
        var keys = new HashSet<String>();
        for (var statement : statements) {
            for (var reference : SqlTableExtractor.extract(statement, dialect)) {
                var key = reference.getAccess() + "|" + reference.getDatabase() + "|" + reference.getSchema() + "|" + reference.getName() + "|" + reference.getKind();
                if (keys.add(key)) {
                    unique.add(reference);
                }
            }
        }

        var truncated = unique.size() > MAX_TABLES;
        var capped = truncated ? new ArrayList<>(unique.subList(0, MAX_TABLES)) : unique;
        return new TableSummary(List.copyOf(validate(connection, capped)), truncated);
    }

    static Dialect dialectOf(Connection connection) {
        try {
            return Dialect.of(connection.getMetaData());
        } catch (SQLException | RuntimeException e) {
            return Dialect.ANSI;
        }
    }

    // Metadata lookups are best effort: drivers such as Pinot, Druid or Arrow Flight do not implement them.
    private static List<TableReference> validate(Connection connection, List<TableReference> references) {
        if (connection == null) {
            return references;
        }
        var validated = new ArrayList<TableReference>(references.size());
        try {
            var metaData = connection.getMetaData();
            for (var reference : references) {
                var isCandidate = reference.getConfidence() == Confidence.MEDIUM && (reference.getKind() == null || reference.getKind() == SqlTableExtractor.Kind.TABLE);
                validated.add(isCandidate && exists(metaData, reference) ? reference.withConfidence(Confidence.HIGH) : reference);
            }
            return validated;
        } catch (SQLException | RuntimeException e) {
            return references;
        }
    }

    private static boolean exists(DatabaseMetaData metaData, TableReference reference) throws SQLException {
        for (var name : List.of(reference.getName(), reference.getName().toUpperCase(Locale.ROOT), reference.getName().toLowerCase(Locale.ROOT))) {
            try (var tables = metaData.getTables(reference.getDatabase(), reference.getSchema(), name, null)) {
                if (tables.next()) {
                    return true;
                }
            }
        }
        return false;
    }
}
