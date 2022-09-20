package io.kestra.plugin.jdbc;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import javax.validation.constraints.NotNull;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcBatch extends AbstractJdbcStatement {
    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source file URI"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @NotNull
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Insert query to be executed",
        description = "The query must have as much question mark as column in the files." +
            "\nExample: 'insert into database values( ? , ? , ? )' for 3 columns" +
            "\nIn case you do not want all columns, you need to precise it in the query and in the columns property" +
            "\nExample: 'insert into(id,name) database values( ? , ? )' to select 2 columns"
    )
    @PluginProperty(dynamic = true)
    private String sql;

    @Schema(
        title = "The size of chunk for every bulk request"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    @NotNull
    private Integer chunk = 1000;

    @Schema(
        title = "The columns to be insert",
        description = "If not provided, `?` count need to match the `from` number of cols"
    )
    @PluginProperty(dynamic = true)
    private List<String> columns;

    protected abstract AbstractCellConverter getCellConverter(ZoneId zoneId);

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from));

        AtomicLong count = new AtomicLong();

        AbstractCellConverter cellConverter = this.getCellConverter(this.zoneId());

        String sql = runContext.render(this.sql);
        logger.debug("Starting prepared statement: {}", sql);

        try (
            Connection connection = this.connection(runContext);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))
        ) {
            connection.setAutoCommit(false);

            Flowable<Integer> flowable = Flowable.create(FileSerde.reader(bufferedReader), BackpressureStrategy.BUFFER)
                .doOnNext(docWriteRequest -> {
                    count.incrementAndGet();
                })
                .buffer(this.chunk, this.chunk)
                .map(o -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ParameterType parameterMetaData = ParameterType.of(ps.getParameterMetaData());

                    for (Object value : o) {
                        ps = this.addRows(ps, parameterMetaData, value, cellConverter, connection);

                        ps.addBatch();
                        ps.clearParameters();
                    }

                    int[] updatedRows = ps.executeBatch();
                    connection.commit();

                    return Arrays.stream(updatedRows).sum();
                });

            Integer updated = flowable
                .reduce(Integer::sum)
                .blockingGet();

            runContext.metric(Counter.of("records", count.get()));
            runContext.metric(Counter.of("updated", updated == null ? 0 : updated));

            logger.info("Successfully bulk {} queries with {} updated rows", count.get(), updated);

            return Output
                .builder()
                .rowCount(count.get())
                .updatedCount(updated)
                .build();
        }
    }

    @SuppressWarnings("unchecked")
    private PreparedStatement addRows(
        PreparedStatement ps,
        ParameterType parameterMetaData,
        Object o,
        AbstractCellConverter cellConverter,
        Connection connection
    ) throws Exception {
        if (o instanceof Map) {
            Map<String, Object> map = ((Map<String, Object>) o);
            ListIterator<String> iterKeys = new ArrayList<>(map.keySet()).listIterator();
            int index = 0;
            while (iterKeys.hasNext()) {
                String col = iterKeys.next();
                if (this.columns == null || this.columns.contains(col)) {
                    index++;
                    ps = cellConverter.addPreparedStatementValue(ps, parameterMetaData, map.get(col), index, connection);
                }
            }
        } else if (o instanceof Collection) {
            ListIterator<Object> iter = ((List<Object>) o).listIterator();

            while (iter.hasNext()) {
                ps = cellConverter.addPreparedStatementValue(ps, parameterMetaData, iter.next(), iter.nextIndex(), connection);
            }
        }

        return ps;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The rows count")
        private final Long rowCount;

        @Schema(title = "The updated rows count")
        private final Integer updatedCount;
    }

    public static class ParameterType {
        private final Map<Integer, Class<?>> cls = new HashMap<>();
        private final Map<Integer, Integer> types = new HashMap<>();
        private final Map<Integer, String> typesName = new HashMap<>();

        public static ParameterType of(ParameterMetaData parameterMetaData) throws SQLException, ClassNotFoundException {
            ParameterType parameterType = new ParameterType();

            for (int i = 1; i <= parameterMetaData.getParameterCount(); i++) {
                parameterType.cls.put(i, Class.forName(parameterMetaData.getParameterClassName(i)));
                parameterType.types.put(i, parameterMetaData.getParameterType(i));
                parameterType.typesName.put(i, parameterMetaData.getParameterTypeName(i));
            }

            return parameterType;
        }

        public Class<?> getClass(int index) {
            return this.cls.get(index);
        }

        public Integer getType(int index) {
            return this.types.get(index);
        }

        public String getTypeName(int index) {
            return this.typesName.get(index);
        }
    }
}
