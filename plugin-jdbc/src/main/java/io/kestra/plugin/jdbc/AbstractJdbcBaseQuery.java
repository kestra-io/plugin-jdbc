package io.kestra.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcBaseQuery extends Task implements JdbcQueryInterface {
    private String url;

    private String username;

    private String password;

    private String timeZoneId;

    protected String sql;

    @Builder.Default
    @Deprecated(since="0.19.0", forRemoval=true)
    private boolean store = false;

    @Deprecated(since="0.19.0", forRemoval=true)
    @Builder.Default
    private boolean fetchOne = false;

    @Deprecated(since="0.19.0", forRemoval=true)
    @Builder.Default
    private boolean fetch = false;

    @NotNull
    @Builder.Default
    protected FetchType fetchType = FetchType.STORE;

    @Builder.Default
    protected Integer fetchSize = 10000;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Object> additionalVars = new HashMap<>();

    private static final ObjectMapper MAPPER = JacksonMapper.ofIon();

    protected abstract AbstractCellConverter getCellConverter(ZoneId zoneId);

    protected Statement createStatement(Connection conn) throws SQLException {
        return conn.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY
        );
    }

    protected String[] tags() {
        return new String[]{
            "fetch", this.getFetchType().equals(FetchType.FETCH) || this.getFetchType().equals(FetchType.FETCH_ONE) ? "true" : "false",
            "store", this.getFetchType().equals(FetchType.STORE) ? "true" : "false",
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
        boolean isResult;
        long count = 0;

        do {
            while (rs.next()) {
                Map<String, Object> map = mapResultSetToMap(rs, cellConverter, connection);
                c.accept(map);
                count++;
            }
            isResult = stmt.getMoreResults();
        } while (isResult);

        return count;
    }

    protected Map<String, Object> mapResultSetToMap(ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        int columnsCount = rs.getMetaData().getColumnCount();
        Map<String, Object> map = new LinkedHashMap<>();

        for (int i = 1; i <= columnsCount; i++) {
            map.put(rs.getMetaData().getColumnName(i), convertCell(i, rs, cellConverter, connection));
        }

        return map;
    }

    private Object convertCell(int columnIndex, ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        return cellConverter.convertCell(columnIndex, rs, connection);
    }

    @Override
    public FetchType getFetchType() {
        if(this.fetch) {
            return FetchType.FETCH;
        } else if(this.fetchOne) {
            return FetchType.FETCH_ONE;
        } else if(this.store) {
            return FetchType.STORE;
        }
        return fetchType;
    }

    public boolean isFetchOne() {
        return this.getFetchType().equals(FetchType.FETCH_ONE);
    }

    public boolean isFetch() {
        return this.getFetchType().equals(FetchType.FETCH);
    }

    public boolean isStore() {
        return this.getFetchType().equals(FetchType.STORE);
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
