package io.kestra.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
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
        title = "SQL statement(s) to execute",
        description = """
            Runs one or more SQL statements rendered with flow variables.
            Query tasks accept a single statement; Queries tasks can execute multiple statements separated by semicolons"""
    )
    @NotNull
    protected Property<String> sql;

    @Schema(
        title = "SQL to execute after main query in same transaction",
        description = """
            Optional SQL executed in the same transaction after the main statement.
            Useful for marking rows as processed to avoid duplicates; only a single statement is allowed. Commit covers both sql and afterSQL"""
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

    @Schema(
        title = "Result fetching mode",
        description = "FETCH returns all rows, FETCH_ONE returns the first row only, STORE streams rows to internal storage (ION), NONE returns no data. Default: NONE"
    )
    @NotNull
    @Builder.Default
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.NONE);

    @Schema(
        title = "Number of rows to fetch per database round trip",
        description = "Controls JDBC fetch size for STORE mode. Default: 10,000 rows; use Integer.MIN_VALUE for MySQL streaming. Ignored for FETCH and FETCH_ONE"
    )
    @Builder.Default
    protected Property<Integer> fetchSize = Property.ofValue(10000);

    @Schema(
        title = "Named parameter bindings for SQL query",
        description = "Map of parameter names to values. Use :name placeholders rendered then bound as prepared-statement parameters; supports nulls and typed values"
    )
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
    }
}
