package io.kestra.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.queues.QueueException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.sql.*;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.kestra.core.models.enums.MonacoLanguages;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcBaseQuery extends Task implements JdbcQueryInterface {

    @PluginProperty(group = "connection")
    private Property<String> url;

    @PluginProperty(group = "connection")
    private Property<String> username;

    @PluginProperty(group = "connection")
    private Property<String> password;

    @PluginProperty(group = "advanced")
    private Property<String> timeZoneId;

    @Schema(
        title = "Deprecated, ignored",
        description = "Deprecated, ignored. Connections are no longer pooled.",
        deprecated = true
    )
    @Deprecated
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> connectionPooling = Property.ofValue(true);

    @Schema(
        title = "Deprecated, ignored",
        description = "Deprecated, ignored. Connections are no longer pooled.",
        deprecated = true
    )
    @Deprecated
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> connectionPoolSize = Property.ofValue(10);

    @Schema(
        title = "SQL statement(s) to execute",
        description = """
            Runs one or more SQL statements rendered with flow variables.
            Query tasks accept a single statement; Queries tasks can execute multiple statements separated by semicolons"""
    )
    @NotNull
    @PluginProperty(language = MonacoLanguages.SQL, group = "main")
    protected Property<String> sql;

    @Schema(
        title = "SQL to execute after main query in same transaction",
        description = """
            Optional SQL executed in the same transaction after the main statement.
            Useful for marking rows as processed to avoid duplicates; only a single statement is allowed. Commit covers both sql and afterSQL"""
    )
    @PluginProperty(language = MonacoLanguages.SQL, group = "advanced")
    protected Property<String> afterSQL;

    /**
     * @deprecated use fetchType: STORE instead
     */
    @Builder.Default
    @Deprecated(since = "0.19.0", forRemoval = true)
    private boolean store = false;

    /**
     * @deprecated use fetchType: FETCH_ONE instead
     */
    @Builder.Default
    @Deprecated(since = "0.19.0", forRemoval = true)
    private boolean fetchOne = false;

    /**
     * @deprecated use fetchType: FETCH instead
     */
    @Builder.Default
    @Deprecated(since = "0.19.0", forRemoval = true)
    private boolean fetch = false;

    @Schema(
        title = "Result fetching mode",
        description = "FETCH returns all rows, FETCH_ONE returns the first row only, STORE streams rows to internal storage (ION), NONE returns no data. Default: NONE"
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.NONE);

    @Schema(
        title = "Number of rows to fetch per database round trip",
        description = "Controls JDBC fetch size for STORE mode. Default: 10,000 rows; use Integer.MIN_VALUE for MySQL streaming. Ignored for FETCH and FETCH_ONE"
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Integer> fetchSize = Property.ofValue(10000);

    @Schema(
        title = "Named parameter bindings for SQL query",
        description = """
            Map of parameter names to values, bound to `:name` placeholders as prepared-statement parameters; supports nulls and typed values. \
            Colons in literals, quoted identifiers, comments and `::` casts are left as is; a `:name` missing from this map is bound to NULL with a warning."""
    )
    @PluginProperty(group = "advanced")
    protected Property<Map<String, Object>> parameters;

    @Schema(
        title = "Output file names to capture after SQL execution",
        description = """
            Creates named temporary files in the task working directory before the SQL runs, \
            making their absolute paths available as `{{ outputFiles.name }}` Pebble variables in the SQL template. \
            Only supported by embedded, in-process drivers (DuckDB, SQLite) where the database \
            engine writes to the same filesystem as the Kestra worker. \
            Remote database drivers (Postgres, MySQL, etc.) do not support this — they execute \
            SQL on a separate server that cannot write to the Kestra worker filesystem."""
    )
    @PluginProperty(group = "destination")
    protected Property<List<String>> outputFiles;

    @Schema(
        title = "Expose the executed SQL statement in the outputs",
        description = """
            When `true`, the rendered SQL is exposed in the `statement` output, with the values of the `secret()` calls used in `sql` replaced by `***` and truncated to 64 KiB.
            Disabled by default so that SQL text never leaks into outputs unless requested"""
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Boolean> outputStatement = Property.ofValue(false);

    @Schema(
        title = "Compute per-column statistics of the fetched rows",
        description = """
            When `true`, writes an ION file with one entry per column (`column`, `jdbcType`, `nulls`, `distinct` (approximate), `min`, `max`, `mean`, `median`, `minLen`, `maxLen`) and exposes its URI in the `columnStats` output.
            Statistics are computed while the rows are read, without running a second query. Only applies to `fetchType` FETCH and STORE; LOB, binary and array columns are skipped.
            `min` and `max` are omitted for string columns longer than 256 characters, and `median` is computed on a bounded sample of the values"""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    protected Property<Boolean> columnStats = Property.ofValue(false);

    @Schema(
        title = "Number of rows used to compute column statistics",
        description = "Only used when `columnStats` is `true`. Statistics stop at this many rows while all rows are still fetched or stored. Must be at least 1. Default: 100,000"
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    protected Property<@Min(1) Integer> columnStatsSampleRows = Property.ofValue(100_000);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Object> additionalVars = new HashMap<>();

    // Set by the triggers: they poll often and publish their output as `trigger.*`, so they skip the execution details.
    @Getter(AccessLevel.NONE)
    protected transient boolean skipExecutionDetails;

    // Set by tasks that render the SQL themselves before calling run(), so the `secret()` calls can still be masked.
    @Getter(AccessLevel.NONE)
    protected transient Property<String> maskingTemplate;

    protected Property<String> secretTemplate() {
        return maskingTemplate != null ? maskingTemplate : this.sql;
    }

    private static final List<String> MULTI_STATEMENT_DRIVERS = List.of(
        "redshift",
        "snowflake",
        "sybase"
    );

    protected abstract AbstractCellConverter getCellConverter(ZoneId zoneId);

    protected Statement createStatement(Connection conn) throws SQLException {
        return conn.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY
        );
    }

    protected String[] tags(RunContext runContext) throws IllegalVariableEvaluationException {
        var fetchTypeRendered = this.renderFetchType(runContext);
        return new String[]{
            "fetch", fetchTypeRendered.equals(FetchType.FETCH) || fetchTypeRendered.equals(FetchType.FETCH_ONE) ? "true" : "false",
            "store", fetchTypeRendered.equals(FetchType.STORE) ? "true" : "false",
        };
    }

    protected ColumnStatsCollector columnStatsCollector(RunContext runContext, FetchType fetchType) throws IllegalVariableEvaluationException {
        var rColumnStats = runContext.render(this.columnStats).as(Boolean.class).orElse(false);
        if (!rColumnStats || (fetchType != FetchType.FETCH && fetchType != FetchType.STORE)) {
            return null;
        }
        var rColumnStatsSampleRows = runContext.render(this.columnStatsSampleRows).as(Integer.class).orElse(100_000);
        if (rColumnStatsSampleRows < 1) {
            throw new IllegalArgumentException("`columnStatsSampleRows` must be at least 1 but was " + rColumnStatsSampleRows);
        }
        return new ColumnStatsCollector(rColumnStatsSampleRows);
    }

    protected boolean renderOutputStatement(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.outputStatement).as(Boolean.class).orElse(false);
    }

    protected Map<String, Object> fetchResult(ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        if (rs.next()) {
            var labels = columnLabels(rs);
            return mapResultSetToMap(rs, labels, cellConverter, connection);
        }
        return null;
    }

    protected long fetchResults(Statement stmt, ResultSet rs, List<Map<String, Object>> maps, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        return fetchResults(stmt, rs, maps, cellConverter, connection, null);
    }

    protected long fetchResults(Statement stmt, ResultSet rs, List<Map<String, Object>> maps, AbstractCellConverter cellConverter, Connection connection, ColumnStatsCollector collector) throws SQLException {
        return fetch(stmt, rs, withStats(Rethrow.throwConsumer(maps::add), collector, rs), cellConverter, connection);
    }

    protected long fetchToFile(Statement stmt, ResultSet rs, OutputStream output, AbstractCellConverter cellConverter, Connection connection) throws SQLException, IOException {
        return fetchToFile(stmt, rs, output, cellConverter, connection, null);
    }

    protected long fetchToFile(Statement stmt, ResultSet rs, OutputStream output, AbstractCellConverter cellConverter, Connection connection, ColumnStatsCollector collector) throws SQLException, IOException {
        return fetch(
            stmt,
            rs,
            withStats(Rethrow.throwConsumer(map -> FileSerde.write(output, map)), collector, rs),
            cellConverter,
            connection
        );
    }

    private static Consumer<Map<String, Object>> withStats(Consumer<Map<String, Object>> consumer, ColumnStatsCollector collector, ResultSet rs) throws SQLException {
        if (collector == null) {
            return consumer;
        }
        collector.describe(rs.getMetaData());
        return map -> {
            collector.accept(map);
            consumer.accept(map);
        };
    }

    protected long fetch(Statement stmt, ResultSet rs, Consumer<Map<String, Object>> c, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        boolean hasMoreResults;
        long count = 0;

        do {
            // Labels are invariant within a single ResultSet; recompute only when a new result set starts.
            var labels = columnLabels(rs);
            while (rs.next()) {
                Map<String, Object> map = mapResultSetToMap(rs, labels, cellConverter, connection);
                c.accept(map);
                count++;
            }
            hasMoreResults = stmt.getMoreResults();
            if (hasMoreResults) {
                // getMoreResults() closes the current ResultSet, so read the next one from the statement.
                rs = stmt.getResultSet();
            }
        } while (hasMoreResults);

        return count;
    }

    // Keep the original signature so any external caller compiled against the old API still works.
    protected Map<String, Object> mapResultSetToMap(ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        return mapResultSetToMap(rs, columnLabels(rs), cellConverter, connection);
    }

    Map<String, Object> mapResultSetToMap(ResultSet rs, String[] labels, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        var map = new LinkedHashMap<String, Object>(labels.length * 2);
        for (int i = 1; i <= labels.length; i++) {
            map.put(labels[i - 1], convertCell(i, rs, cellConverter, connection));
        }
        return map;
    }

    String[] columnLabels(ResultSet rs) throws SQLException {
        var meta = rs.getMetaData();
        var labels = new String[meta.getColumnCount()];
        for (int i = 0; i < labels.length; i++) {
            labels[i] = meta.getColumnLabel(i + 1); // JDBC columns are 1-based
        }
        return labels;
    }

    private Object convertCell(int columnIndex, ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        return cellConverter.convertCell(columnIndex, rs, connection);
    }

    public FetchType renderFetchType(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.fetch) {
            return FetchType.FETCH;
        } else if (this.fetchOne) {
            return FetchType.FETCH_ONE;
        } else if (this.store) {
            return FetchType.STORE;
        }
        return runContext.render(fetchType).as(FetchType.class).orElseThrow();
    }

    protected PreparedStatement prepareStatement(final RunContext runContext,
                                                 final Connection conn,
                                                 final String sql) throws SQLException, IllegalVariableEvaluationException {

        if (this.getParameters() == null) {
            return createPreparedStatement(conn, sql);
        }

        // Inject named parameters (ex: ':param')
        var rParameters = runContext.render(this.getParameters()).asMap(String.class, Object.class);

        // casts, literals and comments are skipped, see NamedParameterSql
        var prepared = NamedParameterSql.parse(sql, this.backslashEscapesInLiterals(), this.hashLineComments());

        var stmt = createPreparedStatement(conn, prepared.sql());

        var names = prepared.names();
        var undeclared = new LinkedHashSet<String>();
        for (var i = 0; i < names.size(); i++) {
            var name = names.get(i);
            if (!rParameters.containsKey(name)) {
                undeclared.add(name);
            }
            stmt.setObject(i + 1, rParameters.get(name));
        }

        if (!undeclared.isEmpty()) {
            runContext.logger().warn("Named parameters {} have no value in `parameters` after rendering (null or not declared) and are bound to NULL", undeclared);
        }

        return stmt;
    }

    /**
     * Whether a backslash escapes the next character inside a quoted SQL literal ({@code 'it\'s'}).
     * Standard SQL only doubles the quote; MySQL, MariaDB, ClickHouse and Snowflake opt in.
     */
    protected boolean backslashEscapesInLiterals() {
        return false;
    }

    /**
     * Whether {@code #} starts a line comment, as in MySQL, MariaDB and ClickHouse.
     */
    protected boolean hashLineComments() {
        return false;
    }

    protected SqlTableExtractor.Syntax sqlSyntax() {
        return new SqlTableExtractor.Syntax(this.backslashEscapesInLiterals(), this.hashLineComments());
    }

    protected PreparedStatement createPreparedStatement(final Connection conn, final String sql) throws SQLException {
        return conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    protected boolean supportsMultiStatements(Connection conn) {
        try {
            String driver = conn.getMetaData().getDriverName().toLowerCase();
            String product = conn.getMetaData().getDatabaseProductName().toLowerCase();
            String url = conn.getMetaData().getURL().toLowerCase();

            boolean nativeSupport = MULTI_STATEMENT_DRIVERS.stream()
                .anyMatch(s -> driver.contains(s) || product.contains(s));

            boolean mysqlCompatible =
                (driver.contains("mysql") || driver.contains("mariadb"))
                    && url.contains("allowmultiqueries=true");

            return nativeSupport || mysqlCompatible;
        } catch (SQLException e) {
            return false;
        }
    }

    protected void kill(Statement statement) {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.cancel();
                statement.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void kill(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Best-effort close: used from a finally block, after any rollback attempt has already run,
    // so a close failure here must not shadow the original exception being propagated.
    protected static void closeConnection(final RunContext runContext, final Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            runContext.logger().warn("Issue when closing the connection : {}", e.getMessage());
        }
    }

    protected abstract Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException;

    /**
     * Get the database name by executing a database-specific query.
     * Different databases have different ways to query the current database name.
     */
    static String getDatabaseName(Connection conn) {
        try {
            String productName = conn.getMetaData().getDatabaseProductName().toLowerCase();
            String query = switch (productName) {
                case String p when p.contains("postgresql") || p.contains("postgres") -> "SELECT current_database()";
                case String p when p.contains("mysql") || p.contains("mariadb") -> "SELECT DATABASE()";
                case String p when p.contains("oracle") -> "SELECT SYS_CONTEXT('USERENV', 'DB_NAME') FROM DUAL";
                case String p when p.contains("sql server") || p.contains("sqlserver") -> "SELECT DB_NAME()";
                case String p when p.contains("db2") -> "SELECT CURRENT SERVER FROM SYSIBM.SYSDUMMY1";
                case String p when p.contains("h2") -> "SELECT DATABASE()";
                case String p when p.contains("sqlite") -> "PRAGMA database_list";
                case String p when p.contains("duckdb") -> "SELECT current_database()";
                case String p when p.contains("snowflake") -> "SELECT CURRENT_DATABASE()";
                case String p when p.contains("redshift") -> "SELECT CURRENT_DATABASE()";
                case String p when p.contains("clickhouse") -> "SELECT currentDatabase()";
                case String p when p.contains("trino") || p.contains("presto") -> "SELECT current_catalog";
                case String p when p.contains("vertica") -> "SELECT CURRENT_DATABASE()";
                case String p when p.contains("sybase") -> "SELECT DB_NAME()";
                case String p when p.contains("hana") -> "SELECT CURRENT_SCHEMA FROM DUMMY";
                case String p when p.contains("apache pinot") || p.contains("pinot") ->
                    null; // Pinot doesn't have a database concept
                case String p when p.contains("apache druid") || p.contains("druid") ->
                    null; // Druid doesn't have a database concept
                case String p when p.contains("apache arrow") || p.contains("arrow") ->
                    null; // Arrow Flight doesn't have a database concept
                default -> null;
            };

            if (query == null) {
                // Fallback: try to get from catalog or schema
                String catalog = conn.getCatalog();
                if (catalog != null && !catalog.isEmpty()) {
                    return catalog;
                }
                String schema = conn.getSchema();
                if (schema != null && !schema.isEmpty()) {
                    return schema;
                }
                return "unknown";
            }

            // Special handling for SQLite which returns a result set with multiple rows
            if (productName.contains("sqlite")) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    if (rs.next()) {
                        // SQLite PRAGMA database_list returns: seq, name, file
                        // We want the "main" database
                        return rs.getString("name");
                    }
                }
                return "unknown";
            }

            // Execute the query to get the database name
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                if (rs.next()) {
                    String dbName = rs.getString(1);
                    return dbName != null ? dbName : "unknown";
                }
            }
            return "unknown";
        } catch (SQLException e) {
            // If query fails, fallback to catalog or schema
            try {
                String catalog = conn.getCatalog();
                if (catalog != null && !catalog.isEmpty()) {
                    return catalog;
                }
                String schema = conn.getSchema();
                if (schema != null && !schema.isEmpty()) {
                    return schema;
                }
            } catch (SQLException ex) {
                // Ignore
            }
            return "unknown";
        }
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "First row of fetched data",
            description = "Only populated when fetchType is FETCH_ONE"
        )
        @JsonInclude
        private final Map<String, Object> row;

        @Schema(
            title = "List of all fetched rows",
            description = "Only populated when fetchType is FETCH"
        )
        private final List<Map<String, Object>> rows;

        @Schema(
            title = "URI of stored results in internal storage",
            description = "Only populated when fetchType is STORE; file is stored in internal storage using ION format"
        )
        private final URI uri;

        @Schema(
            title = "Number of rows fetched",
            description = "Only populated when fetchType is FETCH or STORE"
        )
        private final Long size;

        @Schema(
            title = "Number of rows affected by the statement",
            description = "Reported by the JDBC driver for INSERT, UPDATE, DELETE and similar statements. Not set when the statement returns a result set or when the driver does not report a count"
        )
        private final Long rowsAffected;

        @Schema(
            title = "Size of the stored result file in bytes",
            description = "Only populated when fetchType is STORE"
        )
        private final Long bytes;

        @Schema(
            title = "Statement execution duration in milliseconds",
            description = "Includes reading the result rows"
        )
        private final Long durationMs;

        @Schema(
            title = "Number of SQL warnings raised by the statement",
            description = "Count of the `SQLWarning` chain reported by the driver, for example Postgres notices"
        )
        private final Integer warnings;

        @Schema(
            title = "Tables read or written by the statement",
            description = """
                Detected from the SQL text, so this is a best effort: CTEs, temporary tables and dialect specific syntax can lower the `confidence` or be missed.
                Limited to 50 entries, see `tablesTruncated`"""
        )
        private final List<SqlTableExtractor.TableReference> tables;

        @Schema(
            title = "Whether the `tables` list was cut at 50 entries"
        )
        private final Boolean tablesTruncated;

        @Schema(
            title = "Executed SQL statement",
            description = "Only populated when `outputStatement` is true. Values of `secret()` calls are replaced by `***` and the text is truncated to 64 KiB"
        )
        private final String statement;

        @Schema(
            title = "URI of the column statistics file in internal storage",
            description = "Only populated when `columnStats` is true and fetchType is FETCH or STORE; file is stored in ION format"
        )
        private final URI columnStats;

        // Lets the engine specific outputs that rebuild an Output carry the execution details over.
        public void copyExecutionDetailsTo(OutputBuilder<?, ?> builder) {
            applyDetails(builder, details());
        }

        private JdbcExecutionSupport.Details details() {
            return new JdbcExecutionSupport.Details(
                rowsAffected, bytes, durationMs, warnings,
                new JdbcExecutionSupport.TableSummary(tables, Boolean.TRUE.equals(tablesTruncated)), statement, columnStats
            );
        }

        static void applyDetails(OutputBuilder<?, ?> builder, JdbcExecutionSupport.Details details) {
            builder
                .rowsAffected(details.rowsAffected())
                .bytes(details.bytes())
                .durationMs(details.durationMs())
                .warnings(details.warnings())
                .tables(details.tables().tables())
                .tablesTruncated(details.tables().truncated() ? Boolean.TRUE : null)
                .statement(details.statement())
                .columnStats(details.columnStats());
        }
    }
}
