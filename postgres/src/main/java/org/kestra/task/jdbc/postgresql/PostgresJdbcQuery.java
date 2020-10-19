package org.kestra.task.jdbc.postgresql;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.task.jdbc.AbstractCellConverter;
import org.kestra.task.jdbc.AbstractJdbcQuery;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "PostgresSQL Query Task",
    body = "PostgresSQL Query Task"
)
public class PostgresJdbcQuery extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output> {

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new PostgresCellConverter(zoneId);
    }

    @Override
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new org.postgresql.Driver());
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }
}
