package io.kestra.plugin.jdbc.snowflake;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerEvaluationTest;

@KestraTest
class SnowflakeTriggerTest extends AbstractJdbcTriggerEvaluationTest {
    @Override
    protected Trigger.TriggerBuilder<?, ?> triggerBuilder() {
        return Trigger.builder().type(Trigger.class.getName());
    }
}
