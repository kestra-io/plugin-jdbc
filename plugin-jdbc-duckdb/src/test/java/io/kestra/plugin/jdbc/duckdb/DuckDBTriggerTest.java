package io.kestra.plugin.jdbc.duckdb;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class DuckDBTriggerTest extends AbstractJdbcTriggerTest {
    @Test
    void run() throws Exception {
        var execution = triggerFlow();

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }

    @Override
    protected String getUrl() {
        return null;
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        // do nothing as we init the database manually from the test method.
    }

    @Override
    protected AbstractJdbcTrigger buildTrigger() {
        var builder = Trigger.builder()
            .id(DuckDBTriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .sql(Property.ofValue("SHOW DATABASES;"))
            .fetchType(Property.ofValue(FetchType.FETCH));
        if (getUrl() != null) builder.url(Property.ofValue(getUrl()));
        if (getUsername() != null) builder.username(Property.ofValue(getUsername()));
        if (getPassword() != null) builder.password(Property.ofValue(getPassword()));
        return builder.build();
    }

}
