package io.kestra.plugin.jdbc.hana;

import io.kestra.core.junit.annotations.KestraTest;
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

        var execution = triggerFlow(
            this.getClass().getClassLoader(),
            "flows",
            "hana-listen"
        );

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
}
