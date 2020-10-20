package org.kestra.task.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private boolean fetchToFile = false;

    @Builder.Default
    @InputProperty(
        description = "Whether to fetch only one data row from the query result to the task output." +
            " This parameter is evaluated before 'fetchToFile' and 'fetch'."
        ,
        dynamic = false
    )
    private boolean fetchOne = false;

    @Builder.Default
    @InputProperty(
        description = "Whether to fetch the data from the query result to the task output" +
            " This parameter is evaluated after 'fetchOne' and 'fetchToFile'.",
        dynamic = false
    )
    private boolean fetch = false;

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
        description = "The time zone id to use for date/time manipulation. Defaut value is Z (zulu / UTC)",
        dynamic = false
    )
    private String timeZoneId;

    private AbstractCellConverter cellConverter;

    private static final ObjectMapper MAPPER = JacksonMapper.ofIon();

    protected abstract AbstractCellConverter getCellConverter(ZoneId zoneId);

    /**
     * JDBC driver may be auto-registered. See https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
     *
     * @throws SQLException
     */
    protected abstract void registerDriver() throws SQLException;


    public AbstractJdbcQuery.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        registerDriver();

        ZoneId zoneId = ZoneId.of("Z");
        if (this.timeZoneId != null) {
            zoneId = ZoneId.of(timeZoneId);
        }

        this.cellConverter = getCellConverter(zoneId);

        Connection conn = DriverManager.getConnection(runContext.render(this.url), runContext.render(this.username), runContext.render(this.password));
        Statement stmt = conn.createStatement();

        try {

            boolean isResult = stmt.execute(runContext.render(this.sql));
            ResultSet rs = stmt.getResultSet();

            Output.OutputBuilder output = Output.builder();

            if (isResult) {
                if (this.fetchOne) {
                    output.row(fetchResult(rs));
                } else if (this.fetchToFile) {
                    File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
                    BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
                    long size = fetchToFile(stmt, rs, fileWriter);
                    fileWriter.close();
                    output
                        .uri(runContext.putTempFile(tempFile))
                        .size(size);
                } else if (this.fetch) {
                    List<Map<String, Object>> maps = new ArrayList<>();
                    long size = fetchResults(stmt, rs, maps);
                    output
                        .rows(maps)
                        .size(size);
                }
            }

            return output.build();
        } finally {
            stmt.close();
        }
    }

    protected Map<String, Object> fetchResult(ResultSet rs) throws SQLException {
        rs.next();
        return mapResultSetToMap(rs);
    }

    protected long fetchResults(Statement stmt, ResultSet rs, List<Map<String, Object>> maps) throws SQLException, IOException {
        return fetch(stmt, rs, Rethrow.throwConsumer(maps::add));
    }

    private long fetchToFile(Statement stmt, ResultSet rs, BufferedWriter writer) throws SQLException, IOException {
        return fetch(stmt, rs, Rethrow.throwConsumer(map -> {
            final String s = MAPPER.writeValueAsString(map);
            writer.write(s);
            writer.write("\n");
        }));
    }

    private long fetch(Statement stmt, ResultSet rs, Consumer<Map<String, Object>> c) throws SQLException, IOException {
        boolean isResult = false;
        long count = 0;

        do {
            while (rs.next()) {
                Map<String, Object> map = mapResultSetToMap(rs);
                c.accept(map);
                count++;
            }
            isResult = stmt.getMoreResults();
        } while (isResult);

        return count;
    }

    private Map<String, Object> mapResultSetToMap(ResultSet rs) throws SQLException {
        int columnsCount = rs.getMetaData().getColumnCount();
        Map<String, Object> map = new HashMap<>();

        for (int i = 1; i <= columnsCount; i++) {
            map.put(rs.getMetaData().getColumnName(i), convertCell(i, rs));
        }

        return map;
    }

    private Object convertCell(int columnIndex, ResultSet rs) throws SQLException {
        return cellConverter.convertCell(columnIndex, rs);
    }

    /**
     * Input or Output can nested as you need
     */
    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {

        @OutputProperty(
            description = "Map containing the first row of fetched data",
            body = "Only populated if 'fetchOne' parameter is set to true."
        )
        private Map<String, Object> row;

        @OutputProperty(
            description = "Lit of map containing rows of fetched data",
            body = "Only populated if 'fetch' parameter is set to true."
        )
        private List<Map<String, Object>> rows;

        @OutputProperty(
            description = "The url of the result file on kestra storage (.ion file / Amazon Ion text format)",
            body = "Only populated if 'fetchToFile' is set to true."
        )
        private URI uri;

        @OutputProperty(
            description = "The size of the fetched rows",
            body = "Only populated if 'fetchToFile' or 'fetch' parameter is set to true."
        )
        private Long size;
    }
}
