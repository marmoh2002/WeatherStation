package com.my_prod;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;
import java.util.Random;
import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import com.weather.model.StatusMessage;
import com.weather.model.WeatherReading;
import com.weather.config.AppConfigs;

public class ProducerApp {
    private static final Logger logger = LogManager.getLogger(ProducerApp.class);

    private static final AtomicLong sequenceNumber = new AtomicLong(0);
    private static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {

        String producerId = UUID.randomUUID().toString().substring(0, 8);
        String clientId = AppConfigs.producerApplicationID + "-" + producerId;
        long stationId = Math.abs(UUID.randomUUID().getMostSignificantBits() % 1000); // unique long per container

        logger.info("Producer " + clientId + " started — station ID: " + stationId);

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.producerApplicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", AppConfigs.getSchemaRegistryUrl());
        KafkaProducer<Long, StatusMessage> producer = new KafkaProducer<Long, StatusMessage>(props);
        logger.info("Start sending messages...");

        // ── Scheduler: fires every 1 second ─────────────────────────────────
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {

            StatusMessage message = synthesizeMessage(stationId);

            if (message == null) {
                // null means this message was dropped (10% drop rate)
                logger.info("Message dropped (simulated)");
                return;
            }

            ProducerRecord<Long, StatusMessage> record = new ProducerRecord<>(AppConfigs.getTopicName(), stationId,
                    message);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send message: " + exception.getMessage());
                } else {
                    logger.info("Sent to partition " + metadata.partition()
                            + " offset " + metadata.offset());
                }
            });

        }, 0, 1, TimeUnit.SECONDS);

        // ── Keep the app alive ───────────────────────────────────────────────
        // we run until the JVM is killed
        // (e.g. docker stop), then clean up gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received — stopping producer...");
            scheduler.shutdown();
            producer.flush();
            producer.close();
            logger.info("Producer closed cleanly.");
        }));

        // Block main thread forever (scheduler runs on its own thread)
        Thread.currentThread().join();

        logger.info("Finished sending messages...");
        producer.flush();
        logger.info("Closing producer...");
        producer.close();

    }

    private static StatusMessage synthesizeMessage(long stationId) {
        if (random.nextDouble() < 0.10) {
            return null; // 10% drop rate
        }
        WeatherReading weather = WeatherReading.newBuilder()
                .setHumidity(random.nextInt(101)) // 0 – 100 %
                .setTemperature(random.nextInt(121) - 20) // -20 – 100 °F
                .setWindSpeed(random.nextInt(151)).build(); // 0 – 150 km/h
        return StatusMessage.newBuilder()
                .setStationId(stationId)
                .setSNo(sequenceNumber.incrementAndGet())
                .setBatteryStatus(randomBatteryStatus())
                .setStatusTimestamp(Instant.now().getEpochSecond())
                .setWeather(weather)
                .build();
    }

    private static String randomBatteryStatus() {
        double roll = random.nextDouble();
        if (roll < 0.30)
            return "low";
        if (roll < 0.70)
            return "medium";
        return "high";
    }
}
