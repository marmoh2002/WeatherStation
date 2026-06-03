package com.weather.consumer;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.weather.config.AppConfigs;
import com.weather.model.StatusMessage;
import com.weather.pipeline.Pipeline;

public class ConsumerLoop implements Runnable {
    private static final Logger logger = LogManager.getLogger(ConsumerLoop.class);
    private final KafkaConsumer<Long, StatusMessage> consumer;
    private final Pipeline pipeline;

    public ConsumerLoop(KafkaConsumer<Long, StatusMessage> consumer, Pipeline pipeline) {
        this.consumer = consumer;
        this.pipeline = pipeline;
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(AppConfigs.getConsumerTopicName()));
        try {
            while (true) {
                ConsumerRecords<Long, StatusMessage> records = consumer.poll(java.time.Duration.ofMillis(1000));
                records.forEach(record -> {
                    logger.debug("Received message: " + record.value().getStationId() + " - "
                            + record.value().getWeather().getTemperature() + "°C, "
                            + record.value().getWeather().getHumidity() + "%" + " at "
                            + record.value().getStatusTimestamp());
                    pipeline.process(record.value());
                });
            }

        } catch (WakeupException e) {
            // We expect this exception when shutting down. We catch it to prevent a stack
            // trace.
            logger.info("WakeupException caught. Leaving the polling loop...");
        } catch (Exception e) {
            logger.error("An unexpected error occurred: " + e.getMessage());
        } finally {
            logger.info("Closing the consumer gracefully...");
            consumer.close(); // always runs, even on exception
        }
    }
}
