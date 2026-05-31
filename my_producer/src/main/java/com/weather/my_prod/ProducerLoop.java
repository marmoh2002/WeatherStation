package com.weather.my_prod;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.weather.config.AppConfigs;
import com.weather.model.StatusMessage;
import com.weather.model.WeatherReading;

public class ProducerLoop implements Runnable {
    private static final Logger logger = LogManager.getLogger(ProducerLoop.class);
    private static final AtomicLong sequenceNumber = new AtomicLong(0);
    private static final Random random = new Random();
    private final KafkaProducer<Long, StatusMessage> producer;
    private ScheduledExecutorService scheduler;

    public ProducerLoop(KafkaProducer<Long, StatusMessage> producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        long stationId = Math.abs(UUID.randomUUID().getMostSignificantBits() % 1000); // unique long per container
        String producerId = UUID.randomUUID().toString().substring(0, 8);
        String clientId = AppConfigs.producerApplicationID + "-" + producerId;
        logger.info("Producer " + clientId + " started — station ID: " + stationId);
        scheduler = Executors.newSingleThreadScheduledExecutor();
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
    }

    public void stop() {
        logger.info("Stopping the scheduler and closing producer...");
        if (scheduler != null) {
            scheduler.shutdown();
        }
        producer.flush(); // Force any waiting messages to send
        producer.close(); // Gracefully close
        logger.info("Producer closed cleanly.");
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