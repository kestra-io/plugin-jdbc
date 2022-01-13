package io.kestra.plugin.jdbc.vectorwise;

import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcCopyIn;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class CopyIn extends AbstractJdbcCopyIn implements RunnableTask<AbstractJdbcCopyIn.Output> {
    @Override
    protected void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.ingres.jdbc.IngresDriver());
    }

    @Override
    public AbstractJdbcCopyIn.Output run(RunContext runContext) throws Exception {
        return super.run(runContext);
    }

}


