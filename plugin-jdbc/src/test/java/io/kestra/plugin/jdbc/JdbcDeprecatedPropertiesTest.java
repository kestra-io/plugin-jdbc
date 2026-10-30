package io.kestra.plugin.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.runners.RunContext;
import io.kestra.core.services.FlowService;

import jakarta.inject.Inject;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

@KestraTest
class JdbcDeprecatedPropertiesTest {
    @Inject
    private FlowService flowService;

    @Test
    void unsetPoolingPropertiesAreNotReportedAsDeprecated() {
        assertThat(
            flowService.deprecationPaths(
                flow(
                    QueryTask.builder().id("query").type(QueryTask.class.getName()).sql(Property.ofValue("SELECT 1")).build(),
                    AbstractJdbcBatchStreamingTest.StreamingBatch.builder().id("batch").type(AbstractJdbcBatchStreamingTest.StreamingBatch.class.getName())
                        .from(Property.ofValue("kestra:///f.ion")).table(Property.ofValue("t")).build(),
                    JdbcTriggerTest.DoNothingTrigger.builder().id("trigger").type(JdbcTriggerTest.DoNothingTrigger.class.getName()).sql(Property.ofValue("SELECT 1")).build()
                )
            ), is(empty())
        );
    }

    @Test
    void explicitPoolingPropertiesAreStillReportedAsDeprecated() {
        var query = QueryTask.builder().id("query").type(QueryTask.class.getName()).sql(Property.ofValue("SELECT 1"))
            .connectionPooling(Property.ofValue(true)).connectionPoolSize(Property.ofValue(5)).build();
        var batch = AbstractJdbcBatchStreamingTest.StreamingBatch.builder().id("batch").type(AbstractJdbcBatchStreamingTest.StreamingBatch.class.getName())
            .from(Property.ofValue("kestra:///f.ion")).table(Property.ofValue("t"))
            .connectionPooling(Property.ofValue(true)).connectionPoolSize(Property.ofValue(5)).build();
        var trigger = JdbcTriggerTest.DoNothingTrigger.builder().id("trigger").type(JdbcTriggerTest.DoNothingTrigger.class.getName()).sql(Property.ofValue("SELECT 1"))
            .connectionPooling(Property.ofValue(true)).connectionPoolSize(Property.ofValue(5)).build();

        assertThat(
            flowService.deprecationPaths(flow(query, batch, trigger)), containsInAnyOrder(
                "tasks[0].connectionPooling", "tasks[0].connectionPoolSize",
                "tasks[1].connectionPooling", "tasks[1].connectionPoolSize",
                "triggers[0].connectionPooling", "triggers[0].connectionPoolSize"
            )
        );
    }

    private static Flow flow(Task query, Task batch, AbstractTrigger trigger) {
        return Flow.builder()
            .id("deprecated-pooling")
            .namespace("io.kestra.tests")
            .tasks(List.of(query, batch))
            .triggers(List.of(trigger))
            .build();
    }

    @NoArgsConstructor
    @SuperBuilder
    public static class QueryTask extends AbstractJdbcQuery {
        @Override
        protected AbstractCellConverter getCellConverter(ZoneId zoneId) {
            return new AbstractCellConverter(zoneId) {
                @Override
                public Object convertCell(int columnIndex, ResultSet rs, Connection connection) {
                    return null;
                }
            };
        }

        @Override
        protected Integer getFetchSize(RunContext runContext) {
            return 1;
        }

        @Override
        public void registerDriver() {
        }

        @Override
        public String getScheme() {
            return "jdbc:h2";
        }
    }
}
