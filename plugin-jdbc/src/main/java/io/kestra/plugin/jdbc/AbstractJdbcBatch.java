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

import javax.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcBatch extends AbstractJdbcConnection {
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
    private Integer chunk = 1000;

    @Schema(
        title = "The columns to be insert"
    )
    @PluginProperty(dynamic = true)
    private ArrayList<String> columns;

    @Schema(
        title = "The time zone id to use for date/time manipulation. Default value is the worker default zone id."
    )
    private String timeZoneId;

    protected abstract AbstractCellConverter getCellConverter(ZoneId zoneId);

    @SuppressWarnings("unchecked")
    private PreparedStatement addInsert(PreparedStatement ps, Object o, AbstractCellConverter cellConverter, Connection connection) throws Exception {
        if (o instanceof Map) {
            Map m = ((Map<String, Object>) o);
            ListIterator iterKeys = new ArrayList<>(m.keySet()).listIterator();
            int index = 0;
            while (iterKeys.hasNext()) {
                String col = (String) iterKeys.next();
                if (this.columns == null || this.columns.contains(col)) {
                    index++;
                    ps = cellConverter.adaptedStatement(ps, m.get(col), index, connection);
                }
            }
        } else if (o instanceof Collection) {
            ListIterator iter = ((List<Object>) o).listIterator();

            while (iter.hasNext()) {
                ps = cellConverter.adaptedStatement(ps, iter.next(), iter.nextIndex(), connection);
            }
        }
        ps.addBatch();
        return ps;
    }

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from));

        AtomicLong count = new AtomicLong();


        ZoneId zoneId = TimeZone.getDefault().toZoneId();
        if (this.timeZoneId != null) {
            zoneId = ZoneId.of(timeZoneId);
        }

        AbstractCellConverter cellConverter = this.getCellConverter(zoneId);

        try (
            Connection connection = this.connection(runContext);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))
        ) {
            Flowable flowable = Flowable.create(FileSerde.reader(bufferedReader), BackpressureStrategy.BUFFER)
                .doOnNext(docWriteRequest -> {
                    count.incrementAndGet();
                })
                .buffer(this.chunk, this.chunk)
                .map(o -> {
                    PreparedStatement ps = connection.prepareStatement(this.sql);
                    for (Object value : o) {
                        ps = this.addInsert(ps, value, cellConverter, connection);
                    }
                    ps.executeBatch();
                    return o;
                });

            flowable.count().blockingGet();

            runContext.metric(Counter.of(
                "records", count.get()
            ));

            logger.info(
                "Successfully inserted {} records",
                count.get()
            );
            return Output
                .builder()
                .rowCount(count.get())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The rows count from this `COPY`"
        )
        private final Long rowCount;
    }
}
