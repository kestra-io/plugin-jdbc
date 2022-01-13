package io.kestra.plugin.jdbc.mysql;


import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcBatch;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class Batch extends AbstractJdbcBatch implements RunnableTask<AbstractJdbcBatch.Output> {

    @Override
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
    }

    @Override
    public AbstractJdbcBatch.Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }

}


