package io.kestra.plugin.jdbc;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.micronaut.http.hateoas.Link;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcCopyIn extends AbstractJdbcConnection {
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

    private PreparedStatement addInsert(PreparedStatement ps, Object o) throws Exception {
        if (o instanceof Map) {
            Map m = ((Map<String, Object>) o);
            ListIterator iterKeys = new ArrayList<>(m.keySet()).listIterator();
            Integer index = 0;
            while (iterKeys.hasNext()) {
                String col = (String) iterKeys.next();
                if (this.columns == null || this.columns.contains(col)) {
                    index++;
                    ps = addColumnValue(ps, m.get(col), index);
                }
            }
        } else if (o instanceof Collection) {
            ListIterator iter = ((List<Object>) o).listIterator();

            while (iter.hasNext()) {
                ps = addColumnValue(ps, iter.next(), iter.nextIndex());
            }
        }
        ps.addBatch();
        return ps;
    }

    private PreparedStatement addColumnValue(PreparedStatement ps, Object prop, Integer index) throws Exception {
        if (prop instanceof Integer) {
            ps.setInt(index, (Integer) prop);
            return ps;
        } else if (prop instanceof String) {
            ps.setString(index, (String) prop);
            return ps;
        } else if (prop instanceof String) {
            ps.setString(index, (String) prop);
            return ps;
        } else if (prop instanceof Long) {
            ps.setLong(index, (Long) prop);
            return ps;
        } else if (prop instanceof BigDecimal) {
            ps.setBigDecimal(index, (BigDecimal) prop);
            return ps;
        } else if (prop instanceof Double) {
            ps.setDouble(index, (Double) prop);
            return ps;
        } else if (prop instanceof LocalDateTime) {
            ps.setTimestamp(index, Timestamp.valueOf((LocalDateTime) prop));
            return ps;
        } else if (prop instanceof LocalDate) {
            ps.setDate(index, Date.valueOf((LocalDate) prop));
            return ps;
        } else if (prop instanceof LocalTime) {
            ps.setTime(index, Time.valueOf((LocalTime) prop));
            return ps;
        } else if (prop instanceof ZonedDateTime) {
            ps.setTimestamp(index, Timestamp.valueOf((((ZonedDateTime) prop)).toLocalDateTime()));
            return ps;
        } else if (prop instanceof Boolean) {
            ps.setBoolean(index, (Boolean) prop);
            return ps;
        }
        return ps;
    }


    @SuppressWarnings("unchecked")
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.from));

        AtomicLong count = new AtomicLong();

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
                    for (int cpt = 0; cpt < o.size(); cpt++) {
                        ps = this.addInsert(ps, o.get(cpt));
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
