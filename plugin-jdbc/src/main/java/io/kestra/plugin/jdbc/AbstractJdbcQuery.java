package io.kestra.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

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
import java.util.*;
import java.util.function.Consumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcQuery extends Task implements JdbcQueryInterface {
    private String url;

    private String username;

    private String password;

    private String timeZoneId;

    private String sql;

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
    protected FetchType fetchType = FetchType.NONE;

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

    public AbstractJdbcQuery.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        AbstractCellConverter cellConverter = getCellConverter(this.zoneId());

        try (
            Connection conn = this.connection(runContext);
            Statement stmt = this.createStatement(conn);
        ) {
            if (this instanceof AutoCommitInterface) {
                if (this.getFetchType().equals(FetchType.STORE)) {
                    conn.setAutoCommit(false);
                } else {
                    conn.setAutoCommit(((AutoCommitInterface) this).getAutoCommit());
                }
            }

            stmt.setFetchSize(this.getFetchSize());

            String sql = runContext.render(this.sql, this.additionalVars);
            logger.debug("Starting query: {}", sql);

            boolean isResult = stmt.execute(sql);

            try(ResultSet rs = stmt.getResultSet()) {
                Output.OutputBuilder<?, ?> output = Output.builder();
                long size = 0;

                if (isResult) {
                    switch (this.getFetchType()) {
                        case FETCH_ONE -> {
                            output
                                .row(fetchResult(rs, cellConverter, conn))
                                .size(1L);
                            size = 1;
                        }
                        case STORE -> {
                            File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                            try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile),FileSerde.BUFFER_SIZE)) {
                                size = fetchToFile(stmt, rs, fileWriter, cellConverter, conn);
                            }
                            output
                                .uri(runContext.storage().putFile(tempFile))
                                .size(size);
                        }
                        case FETCH -> {
                            List<Map<String, Object>> maps = new ArrayList<>();
                            size = fetchResults(stmt, rs, maps, cellConverter, conn);
                            output
                                    .rows(maps)
                                    .size(size);
                        }
                    }
                }

                runContext.metric(Counter.of("fetch.size",  size, this.tags()));

                return output.build();
            }
        }
    }

    private String[] tags() {
        return new String[]{
            "fetch", this.fetchType.equals(FetchType.FETCH) || this.fetchType.equals(FetchType.FETCH_ONE) ? "true" : "false",
            "store", this.fetchType.equals(FetchType.STORE) ? "true" : "false",
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

    private long fetch(Statement stmt, ResultSet rs, Consumer<Map<String, Object>> c, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
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

    private Map<String, Object> mapResultSetToMap(ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
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

    public void setFetchOne(boolean fetchOne) {
        if(fetchOne) {
            this.fetchType = FetchType.FETCH_ONE;
        }
    }

    public boolean isFetchOne() {
        return this.fetchType.equals(FetchType.FETCH_ONE);
    }

    public void setFetch(boolean fetch) {
        if(fetch) {
            this.fetchType = FetchType.FETCH;
        }
    }

    public boolean isFetch() {
        return this.fetchType.equals(FetchType.FETCH);
    }

    public void setStore(boolean store) {
        if(store) {
            this.fetchType = FetchType.STORE;
        }
    }

    public boolean isStore() {
        return this.fetchType.equals(FetchType.STORE);
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
