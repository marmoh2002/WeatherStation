package com.weather.my_prod;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.weather.config.AppConfigs;
import com.weather.model.StatusMessage;
import com.weather.model.WeatherReading;

public class ProducerLoop implements Runnable {
    private static final Logger logger = LogManager.getLogger(ProducerLoop.class);
    private static final Random random = new Random();
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    private final Producer<Long, StatusMessage> producer;
    private final LongSupplier stationIdSupplier;
    private ScheduledExecutorService scheduler;
    private volatile long stationId = -1L;
    private boolean stopped = false;

    public ProducerLoop(Producer<Long, StatusMessage> producer) {
        this(producer, StationIdAllocator::allocateStationId);
    }

    ProducerLoop(Producer<Long, StatusMessage> producer, LongSupplier stationIdSupplier) {
        this.producer = producer;
        this.stationIdSupplier = stationIdSupplier;
    }

    long getStationId() {
        return stationId;
    }

    @Override
    public void run() {
        long assignedStationId = stationIdSupplier.getAsLong();
        this.stationId = assignedStationId;
        String producerId = UUID.randomUUID().toString().substring(0, 8);
        String clientId = AppConfigs.producerApplicationID + "-" + producerId;
        logger.info("Producer " + clientId + " started — station ID: " + assignedStationId);
        synchronized (this) {
            if (stopped) {
                return;
            }
            scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> emitOnce(assignedStationId), 0, 1, TimeUnit.SECONDS);
        }
    }

    void emitOnce(long assignedStationId) {
        StatusMessage message = synthesizeMessage(assignedStationId);

        if (message == null) {
            // null means this message was dropped (10% drop rate)
            logger.info("Message dropped (simulated)");
            return;
        }

        ProducerRecord<Long, StatusMessage> record = new ProducerRecord<>(
                AppConfigs.getTopicName(), assignedStationId, message);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to send message: " + exception.getMessage());
            } else {
                logger.info("Sent to partition " + metadata.partition()
                        + " offset " + metadata.offset());
            }
        });
    }

    public void stop() {
        logger.info("Stopping the scheduler and closing producer...");
        synchronized (this) {
            stopped = true;
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
        producer.flush(); // Force any waiting messages to send
        producer.close(); // Gracefully close
        logger.info("Producer closed cleanly.");
    }

    private StatusMessage synthesizeMessage(long assignedStationId) {
        long sNo = sequenceNumber.incrementAndGet();
        if (random.nextDouble() < 0.10) {
            logger.info("Station " + assignedStationId + " dropped packet with sequence no: " + sNo);
            return null;
        }
        WeatherReading weather = WeatherReading.newBuilder()
                .setHumidity(20 + random.nextInt(81)) // 20 – 100 %
                .setTemperature(30 + random.nextInt(81)) // 30 – 110 °F
                .setWindSpeed(random.nextInt(51)) // 0 – 50 km/h
                .build();
        return StatusMessage.newBuilder()
                .setStationId(assignedStationId)
                .setSNo(sNo)
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