package io.kestra.plugin.jdbc.vectorwise;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQueries;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Perform multiple queries on a Vectorwise database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send SQL queries to a Vectorwise database and fetch a row as output.",
            full = true,
            code = """
                   id: vectorwise_queries
                   namespace: company.team

                   tasks:
                     - id: queries
                       type: io.kestra.plugin.jdbc.vectorwise.Queries
                       url: jdbc:vectorwise://url:port/base
                       username: admin
                       password: admin_password
                       sql: select count(*) from employee; select count(*) from laptop;
                       fetchType: FETCH_ONE
                   """
        )
    }
)
public class Queries extends AbstractJdbcQueries implements RunnableTask<AbstractJdbcQueries.MultiQueryOutput> {
    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new VectorwiseCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        DriverManager.registerDriver(new com.ingres.jdbc.IngresDriver());
    }

}