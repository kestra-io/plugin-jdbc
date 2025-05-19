package io.kestra.plugin.jdbc.rockset;

import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Requires(property = "ROCKSET_APIKEY")
@EnabledIfEnvironmentVariable(named = "ROCKSET_APIKEY", matches = ".*")
@Disabled("Rocket didn't exist anymore")
class RocksetTriggerTest extends AbstractJdbcTriggerTest {
    @Property(name = "ROCKSET_APIKEY")
    String apiKey;

    @Test
    void run() throws Exception {
        // trigger the flow
        var execution = triggerFlow(this.getClass().getClassLoader(), "flows","rockset-listen");

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
}