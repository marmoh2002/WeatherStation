package com.weather.consumer;

import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.weather.config.AppConfigs;
import com.weather.model.StatusMessage;
import com.weather.pipeline.Pipeline;

public class ConsumerLoop implements Runnable {
    private static final Logger logger = LogManager.getLogger(ConsumerLoop.class);
    private final KafkaConsumer<Long, StatusMessage> consumer;
    private final Pipeline pipeline;
    private final KafkaProducer<Long, byte[]> dlqProducer;

    public ConsumerLoop(KafkaConsumer<Long, StatusMessage> consumer, Pipeline pipeline) {
        this(consumer, pipeline, BaseStationConsumer.createDeadLetterProducer());
    }

    public ConsumerLoop(KafkaConsumer<Long, StatusMessage> consumer, Pipeline pipeline,
            KafkaProducer<Long, byte[]> dlqProducer) {
        this.consumer = consumer;
        this.pipeline = pipeline;
        this.dlqProducer = dlqProducer;
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(AppConfigs.getConsumerTopicName()));
        boolean paused = false;
        try {
            while (true) {
                Set<TopicPartition> assigned = consumer.assignment();
                if (!paused && pipeline.isParquetQueueHigh() && !assigned.isEmpty()) {
                    logger.warn("Parquet queue reached >=80% capacity ({}/{}). Pausing Kafka consumer.",
                            pipeline.getParquetQueueSize(), Pipeline.PARQUET_QUEUE_CAPACITY);
                    consumer.pause(assigned);
                    paused = true;
                } else if (paused && pipeline.isParquetQueueLow() && !assigned.isEmpty()) {
                    logger.info("Parquet queue drained to {}/{}. Resuming Kafka consumer.",
                            pipeline.getParquetQueueSize(), Pipeline.PARQUET_QUEUE_CAPACITY);
                    consumer.resume(assigned);
                    paused = false;
                }

                ConsumerRecords<Long, StatusMessage> records = consumer.poll(java.time.Duration.ofMillis(1000));
                records.forEach(record -> {
                    StatusMessage msg = record.value();
                    if (msg == null) {
                        Header dlqErr = record.headers().lastHeader(SafeStatusMessageDeserializer.DLQ_ERROR_HEADER);
                        if (dlqErr != null) {
                            routeToDeadLetter(record);
                        }
                        return;
                    }
                    if (msg.getWeather() != null) {
                        logger.debug("Received message: " + msg.getStationId() + " - "
                                + msg.getWeather().getTemperature() + "°F, "
                                + msg.getWeather().getHumidity() + "%" + " at "
                                + msg.getStatusTimestamp());
                    }
                    pipeline.process(msg);
                });
            }

        } catch (WakeupException e) {
            // We expect this exception when shutting down. We catch it to prevent a stack
            // trace.
            logger.info("WakeupException caught. Leaving the polling loop...");
        } catch (Exception e) {
            logger.error("An unexpected error occurred: " + e.getMessage());
        } finally {
            logger.info("Closing the consumer and DLQ producer gracefully...");
            consumer.close(); // always runs, even on exception
            if (dlqProducer != null) {
                dlqProducer.close();
            }
        }
    }

    private void routeToDeadLetter(ConsumerRecord<Long, StatusMessage> record) {
        Header rawPayloadHeader = record.headers().lastHeader(SafeStatusMessageDeserializer.DLQ_RAW_PAYLOAD_HEADER);
        byte[] rawPayload = rawPayloadHeader != null ? rawPayloadHeader.value() : new byte[0];
        String dlqTopic = AppConfigs.getDeadLetterTopicName();
        ProducerRecord<Long, byte[]> dlqRecord = new ProducerRecord<>(dlqTopic, record.key(), rawPayload);
        Header errHeader = record.headers().lastHeader(SafeStatusMessageDeserializer.DLQ_ERROR_HEADER);
        if (errHeader != null) {
            dlqRecord.headers().add(errHeader);
        }
        dlqProducer.send(dlqRecord, (metadata, ex) -> {
            if (ex != null) {
                logger.error("Failed to send malformed message to DLQ topic '{}': {}", dlqTopic, ex.getMessage());
            } else {
                logger.warn("Routed malformed message from partition {} offset {} to DLQ '{}'",
                        record.partition(), record.offset(), dlqTopic);
            }
        });
    }
}
