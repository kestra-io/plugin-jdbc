package io.kestra.plugin.jdbc.hana;

import com.sap.db.jdbc.Driver;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.jdbc.AbstractJdbcQuery;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.sql.DriverManager;
import java.sql.SQLException;

@SuperBuilder
@Getter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Schema(
    title = "Trigger when a SAP HANA query returns rows."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger flow on SAP HANA result",
            full = true,
            code = """
                id: hana_trigger
                namespace: company.team

                triggers:
                  - id: watch
                    type: io.kestra.plugin.jdbc.hana.Trigger
                    interval: "PT1M"
                    url: jdbc:sap://127.0.0.1:39015/?databaseName=SYSTEMDB
                    username: "{{ secret('HANA_USERNAME') }}"
                    password: "{{ secret('HANA_PASSWORD') }}"
                    sql: "SELECT * FROM ALERT_TABLE"
                    fetchType: FETCH
                """
        )
    }
)
public class Trigger extends AbstractJdbcTrigger implements HanaConnectionInterface {

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
            .fetchSize(this.getFetchSize())
            .parameters(this.getParameters())
            .build();

        return query.run(runContext);
    }

    @Override
    public void registerDriver() throws SQLException {
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }
}
