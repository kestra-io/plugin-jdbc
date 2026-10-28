package io.kestra.plugin.jdbc.redshift;

import com.amazon.redshift.jdbc.Driver;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.plugin.jdbc.AbstractCellConverter;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute a single SQL query against Amazon Redshift",
    description = "Runs one SQL statement and fetches results from Amazon Redshift cloud data warehouse. Supports parameterized queries, transactions with afterSQL, and multiple fetch modes (FETCH, FETCH_ONE, STORE). Default fetchSize is 10,000 rows for STORE mode."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a SQL query to a Redshift database and fetch a row as output.",
            full = true,
            code = """
                   id: redshift_query
                   namespace: company.team

                   tasks:
                     - id: select
                       type: io.kestra.plugin.jdbc.redshift.Query
                       url: jdbc:redshift://123456789.eu-central-1.redshift-serverless.amazonaws.com:5439/dev
                       username: "{{ secret('REDSHIFT_USERNAME') }}"
                       password: "{{ secret('REDSHIFT_PASSWORD') }}"
                       sql: SELECT * FROM redshift_types
                       fetchType: FETCH_ONE
                   """
        ),
        @Example(
            title = "Query a provisioned Redshift cluster with IAM authentication instead of a database password.",
            full = true,
            code = """
                   id: redshift_query_iam
                   namespace: company.team

                   tasks:
                     - id: select
                       type: io.kestra.plugin.jdbc.redshift.Query
                       url: jdbc:redshift://my-cluster.abc123xyz789.eu-central-1.redshift.amazonaws.com:5439/dev
                       accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                       secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                       region: eu-central-1
                       dbUser: analytics
                       sql: SELECT current_user, count(*) FROM sales
                       fetchType: FETCH_ONE
                   """
        ),
        @Example(
            title = "Query Redshift Serverless with IAM, using the worker's default AWS credentials (environment, instance profile or IRSA).",
            full = true,
            code = """
                   id: redshift_serverless_iam
                   namespace: company.team

                   tasks:
                     - id: select
                       type: io.kestra.plugin.jdbc.redshift.Query
                       url: jdbc:redshift://my-workgroup.123456789012.eu-central-1.redshift-serverless.amazonaws.com:5439/dev
                       iamAuth: true
                       workgroupName: my-workgroup
                       region: eu-central-1
                       sql: SELECT current_user
                       fetchType: FETCH_ONE
                   """
        )
    },
    metrics = {
        @Metric(
            name = "fetch.size",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of fetched rows."
        ),
        @Metric(
            name = "duration",
            type = Timer.TYPE,
            description = "The duration of the SQL execution, including reading the result rows."
        ),
        @Metric(
            name = "rows.affected",
            type = Counter.TYPE,
            unit = "rows",
            description = "The number of rows affected, as reported by the JDBC driver."
        ),
        @Metric(
            name = "bytes",
            type = Counter.TYPE,
            unit = "bytes",
            description = "The size of the stored result file."
        )
    }
)
public class Query extends AbstractJdbcQuery implements RedshiftConnectionInterface {
    private Property<Boolean> iamAuth;

    @ToString.Exclude
    private Property<String> accessKeyId;

    @ToString.Exclude
    private Property<String> secretKeyId;

    @ToString.Exclude
    private Property<String> sessionToken;

    private Property<String> region;

    private Property<String> dbUser;

    private Property<Boolean> autoCreate;

    private Property<String> workgroupName;

    @Override
    protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
        return new RedshiftCellConverter(zoneId);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }

    @Override
    protected Integer getFetchSize(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.fetchSize).as(Integer.class).orElse(10000);
    }
}
