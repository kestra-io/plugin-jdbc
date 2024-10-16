package io.kestra.plugin.jdbc;

import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.httprpc.sql.Parameters;
import org.slf4j.Logger;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractJdbcQueries extends AbstractJdbcBaseQuery implements JdbcQueriesInterface {
    private Property<Map<String, Object>> parameters;

    private Property<Boolean> transaction;

    public AbstractJdbcQueries.MultiQueryOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        AbstractCellConverter cellConverter = getCellConverter(this.zoneId());

        try (
            Connection conn = this.connection(runContext);
        ) {
            String sqlRendered = runContext.render(this.sql, this.additionalVars);

            Parameters namedParams = Parameters.parse(sqlRendered);
            PreparedStatement stmt = conn.prepareStatement(namedParams.getSQL(), ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);

            Map<String, Object> namedParamsRendered = this.getParameters() == null ? null : this.getParameters().asMap(runContext, String.class, Object.class);
            if(namedParamsRendered != null && !namedParamsRendered.isEmpty()) {
                namedParams.putAll(namedParamsRendered);
                namedParams.apply(stmt);
            }


            stmt.setFetchSize(this.getFetchSize());

            logger.debug("Starting query: {}", sqlRendered);

            boolean hasMoreResult = stmt.execute();
            List<AbstractJdbcQuery.Output> outputList = new LinkedList<>();

            long totalSize = 0L;
            while (hasMoreResult || stmt.getUpdateCount() != -1) {
                AbstractJdbcQuery.Output.OutputBuilder<?, ?> output = AbstractJdbcQuery.Output.builder();
                totalSize += populateResultFromResultSet(runContext, stmt, output, cellConverter, conn);
                outputList.add(output.build());
                hasMoreResult = stmt.getMoreResults();
            }
            runContext.metric(Counter.of("fetch.size",  totalSize, this.tags()));
            return MultiQueryOutput.builder().outputs(outputList).build();
        }
    }

    @Override
    protected long fetch(Statement stmt, ResultSet rs, Consumer<Map<String, Object>> c, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        long count = 0L;

        while (rs.next()) {
            Map<String, Object> map = super.mapResultSetToMap(rs, cellConverter, connection);
            c.accept(map);
            count++;
        }

        return count;
    }

    @SuperBuilder
    @Getter
    public static class MultiQueryOutput implements io.kestra.core.models.tasks.Output {
        List<AbstractJdbcQuery.Output> outputs;
    }
}
