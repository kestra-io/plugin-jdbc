package io.kestra.plugin.jdbc.trino;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import io.trino.jdbc.TrinoDriver;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for query results on Trino and trigger flow",
    description = "Periodically polls Trino by executing a SQL query at the specified interval (default 60 seconds). Triggers a downstream flow execution when the query returns one or more rows. Connection details can be set as typed properties, and the Trino query stats metrics are emitted for each poll. WARNING: Do NOT include a semicolon at the end of your SQL query. Supports parameterized queries and afterSQL. Use fetchType to control result handling."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a SQL query to return results, and then iterate through rows.",
            full = true,
            code = """
                id: jdbc_trigger
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.Loop
                    values: "{{ trigger.rows }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ fromJson(item.value) }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.jdbc.trino.Trigger
                    interval: "PT5M"
                    url: jdbc:trino://localhost:8080/tpch
                    username: "{{ secret('TRINO_USERNAME') }}"
                    password: "{{ secret('TRINO_PASSWORD') }}"
                    sql: "SELECT * FROM my_table"
                    fetchType: FETCH
                """
        )
    }
)
public class Trigger extends AbstractJdbcTrigger implements TrinoConnectionInterface {

    // The typed connection properties are declared on `TrinoConnectionInterface`; like the Snowflake plugin,
    // they are redeclared as Lombok fields here (and on `Query` and `Queries`) so their builder, getter and
    // serialization are generated, and they are forwarded to the `Query` this trigger runs.

    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<String> accessToken;

    @PluginProperty(group = "connection")
    private Property<String> catalog;

    @PluginProperty(group = "connection")
    private Property<String> schema;

    @PluginProperty(group = "connection")
    private Property<Boolean> ssl;

    @PluginProperty(group = "connection")
    private Property<SslVerification> sslVerification;

    @PluginProperty(group = "connection")
    private Property<String> sslKeyStore;

    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<String> sslKeyStorePassword;

    @PluginProperty(group = "connection")
    private Property<String> sslKeyStoreType;

    @PluginProperty(group = "connection")
    private Property<String> sslTrustStore;

    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<String> sslTrustStorePassword;

    @PluginProperty(group = "connection")
    private Property<String> sslTrustStoreType;

    @PluginProperty(group = "connection")
    private Property<String> kerberosPrincipal;

    @PluginProperty(group = "connection")
    private Property<String> kerberosRemoteServiceName;

    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<String> kerberosKeytab;

    @PluginProperty(group = "connection")
    private Property<String> kerberosConfig;

    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> sessionProperties;

    @PluginProperty(group = "advanced")
    private Property<List<String>> clientTags;

    @PluginProperty(group = "advanced")
    @Builder.Default
    private Property<String> source = Property.ofValue("kestra");

    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> roles;

    @PluginProperty(group = "advanced", secret = true)
    @ToString.Exclude
    private Property<Map<String, Object>> extraCredentials;

    @Override
    protected AbstractJdbcQuery.Output runQuery(RunContext runContext) throws Exception {
        var query = Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .url(this.getUrl())
            .username(this.getUsername())
            .password(this.getPassword())
            .timeZoneId(this.getTimeZoneId())
            .sql(this.getSql())
            .afterSQL(this.getAfterSQL())
            .fetch(this.isFetch())
            .store(this.isStore())
            .fetchOne(this.isFetchOne())
            .fetchType(Property.ofValue(this.renderFetchType(runContext)))
            .skipExecutionDetails(true)
            .fetchSize(this.getFetchSize())
            .additionalVars(this.additionalVars)
            .parameters(this.getParameters())
            .catalog(this.getCatalog())
            .schema(this.getSchema())
            .kerberosPrincipal(this.getKerberosPrincipal())
            .kerberosRemoteServiceName(this.getKerberosRemoteServiceName())
            .kerberosConfig(this.getKerberosConfig())
            .sessionProperties(this.getSessionProperties())
            .clientTags(this.getClientTags())
            .source(this.getSource())
            .roles(this.getRoles())
            .ssl(this.getSsl())
            .sslVerification(this.getSslVerification())
            .sslKeyStore(this.getSslKeyStore())
            .sslKeyStoreType(this.getSslKeyStoreType())
            .sslTrustStore(this.getSslTrustStore())
            .sslTrustStoreType(this.getSslTrustStoreType());

        if (this.getAccessToken() != null) {
            query.accessToken(this.getAccessToken());
        }
        if (this.getSslKeyStorePassword() != null) {
            query.sslKeyStorePassword(this.getSslKeyStorePassword());
        }
        if (this.getKerberosKeytab() != null) {
            query.kerberosKeytab(this.getKerberosKeytab());
        }
        if (this.getExtraCredentials() != null) {
            query.extraCredentials(this.getExtraCredentials());
        }

        return query.build().run(runContext);
    }

    @Override
    public void registerDriver() throws SQLException {
        // only register the driver if not already exist to avoid a memory leak
        if (DriverManager.drivers().noneMatch(TrinoDriver.class::isInstance)) {
            DriverManager.registerDriver(new TrinoDriver());
        }
    }
}
