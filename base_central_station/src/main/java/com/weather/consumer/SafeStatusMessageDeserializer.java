package com.weather.consumer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.weather.model.StatusMessage;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

/**
 * Wraps KafkaAvroDeserializer so malformed payloads do not crash consumer.poll().
 * On failure, attaches error metadata and raw bytes to record headers for DLQ routing.
 */
public class SafeStatusMessageDeserializer implements Deserializer<StatusMessage> {
    public static final String DLQ_ERROR_HEADER = "dlq-error";
    public static final String DLQ_RAW_PAYLOAD_HEADER = "dlq-raw-payload";

    private static final Logger logger = LogManager.getLogger(SafeStatusMessageDeserializer.class);
    private final KafkaAvroDeserializer inner = new KafkaAvroDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public StatusMessage deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }

    @Override
    public StatusMessage deserialize(String topic, Headers headers, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            Object obj = inner.deserialize(topic, headers, data);
            if (obj instanceof StatusMessage msg) {
                return msg;
            }
            throw new IllegalArgumentException("Unexpected deserialized type: "
                    + (obj != null ? obj.getClass().getName() : "null"));
        } catch (Exception e) {
            logger.error("Deserialization failed on topic '{}': {}. Routing to DLQ.", topic, e.getMessage());
            if (headers != null) {
                String errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                headers.add(DLQ_ERROR_HEADER, errMsg.getBytes(StandardCharsets.UTF_8));
                headers.add(DLQ_RAW_PAYLOAD_HEADER, data);
            }
            return null;
        }
    }

    @Override
    public void close() {
        inner.close();
    }
}
