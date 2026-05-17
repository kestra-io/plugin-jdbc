package io.kestra.plugin.jdbc.sqlite;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class SqliteTriggerTest extends AbstractJdbcTriggerTest {

    @Test
    void run() throws Exception {
        var execution = triggerFlow();

        var rows = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
        assertThat(rows.size(), is(1));
    }

    @Override
    protected String getUrl() {
        return TestUtils.url();
    }

    @Override
    protected String getUsername() {
        return TestUtils.username();
    }

    @Override
    protected String getPassword() {
        return TestUtils.password();
    }

    @Override
    protected void initDatabase() throws SQLException, FileNotFoundException, URISyntaxException {
        executeSqlScript("scripts/sqlite.sql");
    }

    @Override
    protected AbstractJdbcTrigger buildTrigger() {
        return Trigger.builder()
            .id(SqliteTriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .sql(Property.ofValue("SELECT * FROM lite_types"))
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
    }

}
