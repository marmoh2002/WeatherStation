package com.weather.bitcask.engine;

import org.apache.avro.io.*;
import org.apache.avro.specific.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import com.weather.model.StatusMessage;

public class AvroSerializer {
    public static byte[] serialize(StatusMessage message) throws IOException {
        if (message == null) {
            throw new IllegalArgumentException("Cannot serialize null message");
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DatumWriter<StatusMessage> writer = new SpecificDatumWriter<>(StatusMessage.class);
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(message, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            // Log for debugging
            System.err.println("Failed to serialize StatusMessage: " + e.getMessage());
            throw e; // Re-throw so caller knows about it
        }
    }

    public static StatusMessage deserialize(byte[] bytes) throws IOException {
        DatumReader<StatusMessage> reader = new SpecificDatumReader<>(StatusMessage.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, decoder);
    }
}
