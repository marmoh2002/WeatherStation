package com.weather.bitcask.engine;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import com.weather.model.StatusMessage;

public class AvroSerializer {
    private static final ThreadLocal<SpecificDatumWriter<StatusMessage>> WRITER =
            ThreadLocal.withInitial(() -> new SpecificDatumWriter<>(StatusMessage.class));
    private static final ThreadLocal<SpecificDatumReader<StatusMessage>> READER =
            ThreadLocal.withInitial(() -> new SpecificDatumReader<>(StatusMessage.class));
    private static final ThreadLocal<BinaryEncoder> ENCODER = new ThreadLocal<>();
    private static final ThreadLocal<BinaryDecoder> DECODER = new ThreadLocal<>();
    private static final ThreadLocal<ByteArrayOutputStream> OUTPUT_BUFFER =
            ThreadLocal.withInitial(() -> new ByteArrayOutputStream(256));

    public static byte[] serialize(StatusMessage message) throws IOException {
        if (message == null) {
            throw new IllegalArgumentException("Cannot serialize null message");
        }
        ByteArrayOutputStream out = OUTPUT_BUFFER.get();
        out.reset();
        try {
            BinaryEncoder reuse = ENCODER.get();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, reuse);
            if (encoder != reuse) {
                ENCODER.set(encoder);
            }
            WRITER.get().write(message, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException | RuntimeException e) {
            ENCODER.remove();
            if (e instanceof IOException) {
                // Log for debugging
                System.err.println("Failed to serialize StatusMessage: " + e.getMessage());
            }
            throw e; // Re-throw so caller knows about it
        }
    }

    public static StatusMessage deserialize(byte[] bytes) throws IOException {
        try {
            BinaryDecoder reuse = DECODER.get();
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, reuse);
            if (decoder != reuse) {
                DECODER.set(decoder);
            }
            return READER.get().read(null, decoder);
        } catch (IOException | RuntimeException e) {
            DECODER.remove();
            throw e;
        }
    }
}
