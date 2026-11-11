package io.kestra.plugin.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.serializers.FileSerde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class IonRowWriterTest {
    private static final byte[] ION_VERSION_MARKER = { (byte) 0xE0, 0x01, 0x00, (byte) 0xEA };
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
        var markers = 0;
        for (var i = 0; i <= bytes.length - ION_VERSION_MARKER.length; i++) {
            if (Arrays.equals(bytes, i, i + ION_VERSION_MARKER.length, ION_VERSION_MARKER, 0, ION_VERSION_MARKER.length)) {
                markers++;
            }
        }
        assertThat(markers, is(1));

        var rows = new ArrayList<Object>();
        FileSerde.read(new ByteArrayInputStream(bytes), rows::add);
        assertThat(rows, hasSize(ROWS));
    }
}
