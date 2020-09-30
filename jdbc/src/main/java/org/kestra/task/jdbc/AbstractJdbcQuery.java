package org.kestra.task.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.core.serializers.JacksonMapper;
import org.kestra.core.utils.Rethrow;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Consumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcQuery extends Task {
    @InputProperty(
        description = "The sql query to run",
        dynamic = true
    )
    private String sql;

    @Builder.Default
    @InputProperty(
        description = "Whether to fetch data row from the query result to a file in internal storage." +
            " File will be saved as Amazon Ion (text format)." +
            " \n" +
            " See <a href=\"http://amzn.github.io/ion-docs/\">Amazon Ion documentation</a>" +
            " This parameter is evaluated after 'fetchOne' but before 'fetch'.",
        dynamic = true
    )
    private final boolean store = false;

    @Builder.Default
    @InputProperty(
        description = "Whether to fetch only one data row from the query result to the task output." +
            " This parameter is evaluated before 'store' and 'fetch'."
    )
    private final boolean fetchOne = false;

    @Builder.Default
    @InputProperty(
        description = "Whether to fetch the data from the query result to the task output" +
            " This parameter is evaluated after 'fetchOne' and 'store'."
    )
    private final boolean fetch = false;

    @InputProperty(
        description = "The jdbc url to connect to the database",
        dynamic = true
    )
    private String url;

    @InputProperty(
        description = "The database user",
        dynamic = true
    )
    private String username;

    @InputProperty(
        description = "The database user's password",
        dynamic = true
    )
    private String password;

    @InputProperty(
        description = "The time zone id to use for date/time manipulation. Default value is the worker default zone id."
    )
    private String timeZoneId;

    private static final ObjectMapper MAPPER = JacksonMapper.ofIon();

    protected abstract AbstractCellConverter getCellConverter(ZoneId zoneId);

    /**
     * JDBC driver may be auto-registered. See https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
     *
     * @throws SQLException registerDrivers failed
     */
    protected abstract void registerDriver() throws SQLException;

    public AbstractJdbcQuery.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        registerDriver();

        ZoneId zoneId = TimeZone.getDefault().toZoneId();
        if (this.timeZoneId != null) {
            zoneId = ZoneId.of(timeZoneId);
        }

        AbstractCellConverter cellConverter = getCellConverter(zoneId);

        try (
            Connection conn = DriverManager.getConnection(runContext.render(this.url), runContext.render(this.username), runContext.render(this.password));
            Statement stmt = conn.createStatement()
        ) {
            String sql = runContext.render(this.sql);
            boolean isResult = stmt.execute(sql);

            logger.debug("Starting query: {}", sql);

            ResultSet rs = stmt.getResultSet();

            Output.OutputBuilder output = Output.builder();
            long size = 0;

            if (isResult) {
                if (this.fetchOne) {
                    output
                        .row(fetchResult(rs, cellConverter))
                        .size(1L);
                    size = 1;

                } else if (this.store) {
                    File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
                    BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
                    size = fetchToFile(stmt, rs, fileWriter, cellConverter);
                    fileWriter.close();
                    output
                        .uri(runContext.putTempFile(tempFile))
                        .size(size);
                } else if (this.fetch) {
                    List<Map<String, Object>> maps = new ArrayList<>();
                    size = fetchResults(stmt, rs, maps, cellConverter);
                    output
                        .rows(maps)
                        .size(size);
                }
            }

            runContext.metric(Counter.of("fetch.size",  size, this.tags()));

            return output.build();
        }
    }

    private String[] tags() {
        return new String[]{
            "fetch", this.fetch || this.fetchOne ? "true" : "false",
            "store", this.store ? "true" : "false",
        };
    }

    protected Map<String, Object> fetchResult(ResultSet rs, AbstractCellConverter cellConverter) throws SQLException {
        rs.next();
        return mapResultSetToMap(rs, cellConverter);
    }

    protected long fetchResults(Statement stmt, ResultSet rs, List<Map<String, Object>> maps, AbstractCellConverter cellConverter) throws SQLException {
        return fetch(stmt, rs, Rethrow.throwConsumer(maps::add), cellConverter);
    }

    protected long fetchToFile(Statement stmt, ResultSet rs, BufferedWriter writer, AbstractCellConverter cellConverter) throws SQLException, IOException {
        return fetch(
            stmt,
            rs,
            Rethrow.throwConsumer(map -> {
                final String s = MAPPER.writeValueAsString(map);
                writer.write(s);
                writer.write("\n");
            }),
            cellConverter
        );
    }

    private long fetch(Statement stmt, ResultSet rs, Consumer<Map<String, Object>> c, AbstractCellConverter cellConverter) throws SQLException {
        boolean isResult;
        long count = 0;

        do {
            while (rs.next()) {
                Map<String, Object> map = mapResultSetToMap(rs, cellConverter);
                c.accept(map);
                count++;
            }
            isResult = stmt.getMoreResults();
        } while (isResult);

        return count;
    }

    private Map<String, Object> mapResultSetToMap(ResultSet rs, AbstractCellConverter cellConverter) throws SQLException {
        int columnsCount = rs.getMetaData().getColumnCount();
        Map<String, Object> map = new HashMap<>();

        for (int i = 1; i <= columnsCount; i++) {
            map.put(rs.getMetaData().getColumnName(i), convertCell(i, rs, cellConverter));
        }

        return map;
    }

    private Object convertCell(int columnIndex, ResultSet rs, AbstractCellConverter cellConverter) throws SQLException {
        return cellConverter.convertCell(columnIndex, rs);
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {

        @OutputProperty(
            description = "Map containing the first row of fetched data",
            body = "Only populated if 'fetchOne' parameter is set to true."
        )
        private final Map<String, Object> row;

        @OutputProperty(
            description = "Lit of map containing rows of fetched data",
            body = "Only populated if 'fetch' parameter is set to true."
        )
        private final List<Map<String, Object>> rows;

        @OutputProperty(
            description = "The url of the result file on kestra storage (.ion file / Amazon Ion text format)",
            body = "Only populated if 'store' is set to true."
        )
        private final URI uri;

        @OutputProperty(
            description = "The size of the fetched rows",
            body = "Only populated if 'store' or 'fetch' parameter is set to true."
        )
        private final Long size;
    }
}
