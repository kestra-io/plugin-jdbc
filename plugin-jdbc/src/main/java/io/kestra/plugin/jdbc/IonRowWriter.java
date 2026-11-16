package io.kestra.plugin.jdbc;

import java.io.Closeable;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.ion.IonFactory;
import com.fasterxml.jackson.dataformat.ion.IonGenerator;

import io.kestra.core.serializers.JacksonMapper;

/**
 * Writes rows as one binary Ion stream, flushed per block so memory stays bounded.
 * Core's writer never flushes (local symbol table append is off, auto-flush turns it on), and per-row FileSerde.write repeats the symbol table.
 * Closing it finishes the Ion stream and flushes the underlying output, which stays open for the caller.
 */
public final class IonRowWriter implements Closeable {

    private final IonWriter ionWriter;
    private final IonGenerator generator;
    private final SequenceWriter sequenceWriter;

    private IonRowWriter(IonWriter ionWriter, IonGenerator generator, SequenceWriter sequenceWriter) {
        this.ionWriter = ionWriter;
        this.generator = generator;
        this.sequenceWriter = sequenceWriter;
    }

    public static IonRowWriter of(OutputStream output) throws IOException {
        var ionWriter = IonBinaryWriterBuilder.standard().withAutoFlushEnabled(true).build(new NonClosingOutputStream(output));
        var mapper = JacksonMapper.ofIonBinary();
        var generator = (IonGenerator) ((IonFactory) mapper.getFactory()).createGenerator(ionWriter);
        // Without this, SequenceWriter flushes after every row and defeats block-sized flushing
        var sequenceWriter = mapper.writerFor(JacksonMapper.MAP_TYPE_REFERENCE)
            .without(SerializationFeature.FLUSH_AFTER_WRITE_VALUE)
            .writeValues(generator);
        return new IonRowWriter(ionWriter, generator, sequenceWriter);
    }

    public void write(Map<String, Object> row) throws IOException {
        sequenceWriter.write(row);
    }

    @Override
    public void close() throws IOException {
        try (ionWriter; generator; sequenceWriter) {
            // closed in reverse order: sequence writer, then the generator, then the Ion writer
        }
    }

    // The caller owns the stream (fetchToFile is protected API), so close only flushes.
    private static final class NonClosingOutputStream extends FilterOutputStream {
        NonClosingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            flush();
        }
    }
}
