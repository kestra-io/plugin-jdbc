package io.kestra.plugin.jdbc.druid;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerEvaluationTest;

@KestraTest
class DruidTriggerTest extends AbstractJdbcTriggerEvaluationTest {
    @Override
    protected Trigger.TriggerBuilder<?, ?> triggerBuilder() {
        return Trigger.builder().type(Trigger.class.getName());
    }
}
