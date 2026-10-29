package io.kestra.plugin.jdbc;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class ColumnStatsCollectorTest {

    private static Map<String, Object> row(Object id, Object name) {
        var row = new LinkedHashMap<String, Object>();
        row.put("id", id);
        row.put("name", name);
        return row;
    }

    @Test
    void numericAndStringStats() {
        var collector = new ColumnStatsCollector(100);
        collector.accept(row(1, "a"));
        collector.accept(row(2, "bbb"));
        collector.accept(row(3, null));
        collector.accept(row(null, "bbb"));

        var stats = collector.results();
        var id = stats.stream().filter(s -> "id".equals(s.get("column"))).findFirst().orElseThrow();
        var name = stats.stream().filter(s -> "name".equals(s.get("column"))).findFirst().orElseThrow();

        assertThat(id.get("nulls"), is(1L));
        assertThat(id.get("distinct"), is(3L));
        assertThat(id.get("min"), is(1L));
        assertThat(id.get("max"), is(3L));
        assertThat((Double) id.get("mean"), closeTo(2.0, 0.0001));
        assertThat(id.get("median"), is(2L));
        assertThat(id.get("minLen"), nullValue());

        assertThat(name.get("nulls"), is(1L));
        assertThat(name.get("distinct"), is(2L));
        assertThat(name.get("min"), is("a"));
        assertThat(name.get("max"), is("bbb"));
        assertThat(name.get("mean"), nullValue());
        assertThat(name.get("minLen"), is(1));
        assertThat(name.get("maxLen"), is(3));
    }

    @Test
    void stopsCollectingAfterSampleRows() {
        var collector = new ColumnStatsCollector(2);
        collector.accept(row(1, "a"));
        collector.accept(row(2, "b"));
        collector.accept(row(1000, "z"));

        var id = collector.results().stream().filter(s -> "id".equals(s.get("column"))).findFirst().orElseThrow();

        assertThat(id.get("max"), is(2L));
    }

    @Test
    void longStringsHaveNoMinMax() {
        var collector = new ColumnStatsCollector(10);
        collector.accept(row(1, "x".repeat(ColumnStatsCollector.MAX_MIN_MAX_STRING_LENGTH + 1)));

        var name = collector.results().stream().filter(s -> "name".equals(s.get("column"))).findFirst().orElseThrow();

        assertThat(name.get("min"), nullValue());
        assertThat(name.get("max"), nullValue());
        assertThat(name.get("maxLen"), is(ColumnStatsCollector.MAX_MIN_MAX_STRING_LENGTH + 1));
    }

    @Test
    void distinctIsApproximatelyCorrectOnManyValues() {
        var collector = new ColumnStatsCollector(1_000_000);
        for (var i = 0; i < 50_000; i++) {
            collector.accept(row(i, "v"));
        }

        var id = collector.results().stream().filter(s -> "id".equals(s.get("column"))).findFirst().orElseThrow();

        assertThat(((Long) id.get("distinct")).doubleValue(), closeTo(50_000, 50_000 * 0.05));
    }
}
