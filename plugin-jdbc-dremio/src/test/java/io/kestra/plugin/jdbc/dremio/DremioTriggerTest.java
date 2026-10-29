package io.kestra.plugin.jdbc.dremio;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerEvaluationTest;

@KestraTest
class DremioTriggerTest extends AbstractJdbcTriggerEvaluationTest {
    @Override
    protected Trigger.TriggerBuilder<?, ?> triggerBuilder() {
        return Trigger.builder().type(Trigger.class.getName());
    }
}
