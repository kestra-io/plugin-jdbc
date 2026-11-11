package io.kestra.plugin.jdbc;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.ion.IonFactory;

import io.kestra.core.serializers.JacksonMapper;

/**
 * Writes rows as one binary Ion stream with auto-flush, so memory stays bounded.
 * Closing it finishes the Ion stream and closes the underlying output.
 */
public final class IonRowWriter implements Closeable {

    // Core's binary writer buffers the whole file until close, and per-row FileSerde.write repeats the symbol table.
    private final IonWriter ionWriter;
    private final SequenceWriter sequenceWriter;

    private IonRowWriter(IonWriter ionWriter, SequenceWriter sequenceWriter) {
        this.ionWriter = ionWriter;
        this.sequenceWriter = sequenceWriter;
    }

    public static IonRowWriter of(OutputStream output) throws IOException {
        var ionWriter = IonBinaryWriterBuilder.standard().withAutoFlushEnabled(true).build(output);
        var mapper = JacksonMapper.ofIonBinary();
        var generator = ((IonFactory) mapper.getFactory()).createGenerator(ionWriter);
        return new IonRowWriter(ionWriter, mapper.writerFor(JacksonMapper.MAP_TYPE_REFERENCE).writeValues(generator));
    }

    public void write(Map<String, Object> row) throws IOException {
        sequenceWriter.write(row);
    }

    @Override
    public void close() throws IOException {
        try {
            sequenceWriter.close();
        } finally {
            ionWriter.close();
        }
    }
}
