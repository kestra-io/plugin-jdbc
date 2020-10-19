package org.kestra.task.jdbc.mysql;

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
    description = "MYSQL Query Task",
    body = "MYSQL Query Task"
)
public class MysqlJdbcQuery extends AbstractJdbcQuery implements RunnableTask<AbstractJdbcQuery.Output> {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new MysqlCellConverter(zoneId);
    }

    @Override
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }
}
