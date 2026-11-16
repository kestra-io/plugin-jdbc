package io.kestra.plugin.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.serializers.FileSerde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class IonRowWriterTest {
    private static final int ROWS = 2000;

    @Test
    void streamsToOutputBeforeCloseAsOneIonStream() throws Exception {
        var sink = new ByteArrayOutputStream();
        try (var writer = IonRowWriter.of(sink)) {
            for (var i = 0; i < ROWS; i++) {
                writer.write(Map.of("id", i, "payload", "x".repeat(200)));
            }
            assertThat("bytes must reach the stream before close", sink.size(), greaterThan(0));
        }

        var bytes = sink.toByteArray();
        assertThat(IonTestUtils.countVersionMarkers(bytes), is(1));

        var rows = new ArrayList<Object>();
        FileSerde.read(new ByteArrayInputStream(bytes), rows::add);
        assertThat(rows, hasSize(ROWS));
    }

    @Test
    void leavesCallerStreamOpenAndFlushedAfterClose() throws Exception {
        var closed = new boolean[1];
        var sink = new ByteArrayOutputStream() {
            @Override
            public void close() {
                closed[0] = true;
            }
        };
        try (var writer = IonRowWriter.of(sink)) {
            writer.write(Map.of("id", 1));
            writer.write(Map.of("id", 2));
        }

        assertThat(closed[0], is(false));

        var rows = new ArrayList<Object>();
        FileSerde.read(new ByteArrayInputStream(sink.toByteArray()), rows::add);
        assertThat(rows, hasSize(2));

        var before = sink.size();
        sink.write(0x0A);
        assertThat(sink.size(), is(before + 1));
    }
}
