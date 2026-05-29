package com.weather;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.LongDeserializer;

import com.weather.config.AppConfigs;
import com.weather.model.StatusMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class baseStationConsumer {
    private static final Logger logger = LogManager.getLogger(baseStationConsumer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfigs.getConsumerGroupID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfigs.getSchemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, AppConfigs.isSpecificAvroReader());

        KafkaConsumer<Long, StatusMessage> consumer = new KafkaConsumer<>(props);
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received. Waking up consumer...");

            // This is the ONLY thread-safe method you can call from outside the main
            // thread.
            // It causes consumer.poll() to throw a WakeupException.
            consumer.wakeup();

            // Wait for the main thread to finish its cleanup before letting the JVM die
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for main thread to finish.", e);
            }
        }));

        consumer.subscribe(List.of(AppConfigs.getConsumerTopicName()));
        try {
            while (true) {
                ConsumerRecords<Long, StatusMessage> records = consumer.poll(java.time.Duration.ofMillis(1000));
                records.forEach(record -> {
                    logger.info("Received message: " + record.value().getStationId() + " - "
                            + record.value().getWeather().getTemperature() + "°C, "
                            + record.value().getWeather().getHumidity() + "%" + " at "
                            + record.value().getStatusTimestamp());
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