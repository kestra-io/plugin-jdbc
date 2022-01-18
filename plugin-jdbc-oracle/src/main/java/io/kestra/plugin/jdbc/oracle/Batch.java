package io.kestra.plugin.jdbc.oracle;

import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import io.kestra.core.models.tasks.RunnableTask;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class Batch extends AbstractJdbcBatch implements RunnableTask<AbstractJdbcBatch.Output> {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new OracleCellConverter(zoneId);
    }

    @Override
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
    }

    @Override
    public AbstractJdbcBatch.Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }


}
