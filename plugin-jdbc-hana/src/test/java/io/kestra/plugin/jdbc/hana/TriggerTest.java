package io.kestra.plugin.jdbc.hana;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.plugin.jdbc.AbstractJdbcTrigger;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerTest;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerTest extends AbstractJdbcTriggerTest {

    @Test
    void run() throws Exception {
        // Skip if HANA container is not running
        Assumptions.assumeTrue(
            HanaTestUtils.HANA.isRunning(),
            "SAP HANA container is not running"
        );

        var execution = triggerFlow();

        assertThat(execution != null, is(true));
    }

    @Override
    protected String getUrl() {
        return HanaTestUtils.getJdbcUrl();
    }

    @Override
    protected void initDatabase() {
        // No-op for SAP HANA
    }

    @Override
    protected AbstractJdbcTrigger buildTrigger() {
        return Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .sql(Property.ofValue("SELECT 1 FROM DUMMY"))
            .url(Property.ofValue(getUrl()))
            .username(Property.ofValue(getUsername()))
            .password(Property.ofValue(getPassword()))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
    }

}
