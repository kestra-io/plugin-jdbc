package io.kestra.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.assets.Custom;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.queues.QueueException;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    @PluginProperty(group = "connection")
    private Property<String> timeZoneId;

    @Schema(
        title = "SQL statement(s) to execute.",
        description = """
            Runs one or more SQL statements depending on the task type.
            Query tasks support a single SQL statement, while Queries tasks can run multiple statements separated by semicolons."""
    )
    @NotNull
    protected Property<String> sql;

    @Schema(
        title = "SQL to execute atomically after trigger query.",
        description = """
            Optional SQL executed in the same transaction as the main trigger query.
            Typically updates processing flags to prevent duplicate processing.
            Both sql and afterSQL queries commit together, ensuring consistency."""
    )
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

    @NotNull
    @Builder.Default
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.NONE);

    @Builder.Default
    protected Property<Integer> fetchSize = Property.ofValue(10000);

    protected Property<Map<String, Object>> parameters;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Object> additionalVars = new HashMap<>();

    private static final ObjectMapper MAPPER = JacksonMapper.ofIon();

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

    protected Map<String, Object> fetchResult(ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        if (rs.next()) {
            return mapResultSetToMap(rs, cellConverter, connection);
        }
        return null;
    }

    protected long fetchResults(Statement stmt, ResultSet rs, List<Map<String, Object>> maps, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        return fetch(stmt, rs, Rethrow.throwConsumer(maps::add), cellConverter, connection);
    }

    protected long fetchToFile(Statement stmt, ResultSet rs, BufferedWriter writer, AbstractCellConverter cellConverter, Connection connection) throws SQLException, IOException {
        return fetch(
            stmt,
            rs,
            Rethrow.throwConsumer(map -> {
                final String s = MAPPER.writeValueAsString(map);
                writer.write(s);
                writer.write("\n");
            }),
            cellConverter,
            connection
        );
    }

    protected long fetch(Statement stmt, ResultSet rs, Consumer<Map<String, Object>> c, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        boolean hasMoreResults;
        long count = 0;

        do {
            while (rs.next()) {
                Map<String, Object> map = mapResultSetToMap(rs, cellConverter, connection);
                c.accept(map);
                count++;
            }
            hasMoreResults = stmt.getMoreResults();
        } while (hasMoreResults);

        return count;
    }

    protected Map<String, Object> mapResultSetToMap(ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        int columnsCount = rs.getMetaData().getColumnCount();
        Map<String, Object> map = new LinkedHashMap<>();

        for (int i = 1; i <= columnsCount; i++) {
            map.put(rs.getMetaData().getColumnLabel(i), convertCell(i, rs, cellConverter, connection));
        }

        return map;
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

        // Inject named parameters (ex: ':param')
        Map<String, Object> namedParamsRendered = runContext.render(this.getParameters()).asMap(String.class, Object.class);

        if (namedParamsRendered.isEmpty()) {
            return createPreparedStatement(conn, sql);
        }

        //Extract parameters in orders and replace them with '?'
        String preparedSql = sql;
        Pattern pattern = Pattern.compile(":\\w+");
        Matcher matcher = pattern.matcher(preparedSql);

        List<String> params = new LinkedList<>();

        while (matcher.find()) {
            String param = matcher.group();
            params.add(param.substring(1));
            preparedSql = matcher.replaceFirst("?");
            matcher = pattern.matcher(preparedSql);
        }

        PreparedStatement stmt = createPreparedStatement(conn, preparedSql);

        for (int i = 0; i < params.size(); i++) {
            stmt.setObject(i + 1, namedParamsRendered.get(params.get(i)));
        }

        return stmt;
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

    protected abstract Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException;

    protected void upsertAsset(RunContext runContext, Connection conn, String query) throws SQLException, QueueException, IllegalVariableEvaluationException {
        var table = extractTableNameFromCreateTable(query);
        if (table != null) {
            var system = conn.getMetaData().getDatabaseProductName();
            var database = getDatabaseName(conn);
            Map<String, Object> metadata = Map.of(
                "system", system,
                "database", database,
                "name", table
            );
            try {
                runContext.logger().info("Creating asset for system \"{}\", database \"{}\", table \"{}\"", system, database, table);
                runContext.assets().emit(
                    new AssetEmit(
                        List.of(),
                        List.of(
                            Custom.builder()
                                .id(database + "." + table)
                                .type("io.kestra.plugin.ee.assets.Table")
                                .metadata(metadata)
                                .build()
                        )
                    )
                );
            } catch (UnsupportedOperationException | NullPointerException ignored) {
                // Assets are optional and may be unavailable in OSS/test runtimes.
            }
        }
    }

    /**
     * Get the database name by executing a database-specific query.
     * Different databases have different ways to query the current database name.
     */
    private String getDatabaseName(Connection conn) {
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

    /**
     * Extract the table name from a CREATE TABLE statement.
     * Returns null if the SQL is not a CREATE TABLE statement.
     * The CREATE TABLE syntax is similar across most SQL databases:
     * CREATE [TEMPORARY|TEMP] TABLE [IF NOT EXISTS] [schema.]table_name ...
     */
    private String extractTableNameFromCreateTable(String sql) {
        if (sql == null || sql.isEmpty()) {
            return null;
        }

        // Normalize the SQL: remove extra whitespace and convert to single spaces
        String normalizedSql = sql.trim().replaceAll("\\s+", " ");

        // Check if it's a CREATE TABLE statement (case-insensitive)
        if (!normalizedSql.toUpperCase().startsWith("CREATE ")) {
            return null;
        }

        // Pattern to match CREATE TABLE statements
        // Handles: CREATE TABLE, CREATE TEMP TABLE, CREATE TEMPORARY TABLE, CREATE TABLE IF NOT EXISTS
        String pattern = "(?i)^CREATE\\s+(TEMP|TEMPORARY\\s+)?TABLE\\s+(IF\\s+NOT\\s+EXISTS\\s+)?([\\w.`\"\\[\\]]+)";
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(normalizedSql);

        if (m.find()) {
            String tableName = m.group(3);
            // Clean up the table name: remove quotes, brackets, and schema prefixes if needed
            tableName = tableName
                .replaceAll("[`\"\\[\\]]", "") // Remove quotes and brackets
                .trim();

            // If there's a schema prefix (e.g., schema.table), take only the table name
            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                tableName = parts[parts.length - 1]; // Get the last part (table name)
            }

            return tableName.isEmpty() ? null : tableName;
        }

        return null;
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Map containing the first row of fetched data.",
            description = "Only populated if `fetchOne` parameter is set to true."
        )
        @JsonInclude
        private final Map<String, Object> row;

        @Schema(
            title = "List of map containing rows of fetched data.",
            description = "Only populated if `fetch` parameter is set to true."
        )
        private final List<Map<String, Object>> rows;

        @Schema(
            title = "The URI of the result file on Kestra's internal storage (.ion file / Amazon Ion formatted text file).",
            description = "Only populated if `store` is set to true."
        )
        private final URI uri;

        @Schema(
            title = "The number of rows fetched.",
            description = "Only populated if `store` or `fetch` parameter is set to true."
        )
        private final Long size;
    }
}
