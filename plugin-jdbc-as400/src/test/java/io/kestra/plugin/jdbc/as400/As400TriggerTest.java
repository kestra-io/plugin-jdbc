package io.kestra.plugin.jdbc.as400;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.jdbc.AbstractJdbcTriggerEvaluationTest;

@KestraTest
class As400TriggerTest extends AbstractJdbcTriggerEvaluationTest {
    @Override
    protected Trigger.TriggerBuilder<?, ?> triggerBuilder() {
        return Trigger.builder().type(Trigger.class.getName());
    }
}
