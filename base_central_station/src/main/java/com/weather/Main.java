package com.weather;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.weather.model.StatusMessage;
import com.weather.model.WeatherReading;
import com.weather.archive.engine.ParquetArchiver;
import com.weather.bitcask.engine.BitCaskStore;
import com.weather.bitcask.engine.Compactor;
import com.weather.config.AppConfigs;
import com.weather.consumer.BaseStationConsumer;
import com.weather.consumer.ConsumerLoop;
import com.weather.pipeline.Pipeline;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);
    private static final AtomicLong sequenceNumber = new AtomicLong(0);

    public static void main(String[] args) {
        if (args.length > 0 && args[0].equals("--test-bitcask")) {
            testBitCask();
            return;
        }
        // 1. Start store and archiver
        BitCaskStore store;
        try {
            store = new BitCaskStore(AppConfigs.getStoreDirectory());
        } catch (Exception e) {
            logger.error("Failed to initialize BitCaskStore: ", e);
            return;
        }
        ParquetArchiver archiver;
        try {
            archiver = new ParquetArchiver(AppConfigs.getParquetOutputBasePath());
        } catch (Exception e) {
            logger.error("Failed to initialize ParquetArchiver: ", e);
            return;
        }

        // 2. Wire pipeline
        Pipeline pipeline = new Pipeline(store, archiver);

        // 3. Compactor
        Compactor compactor = new Compactor(store);

        compactor.start();

        // 4. Start consumer — pass pipeline in so poll loop calls pipeline.process()
        KafkaConsumer<Long, StatusMessage> consumer = BaseStationConsumer.createConsumer();
        ConsumerLoop consumerLoop = new ConsumerLoop(consumer, pipeline);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received. Waking up consumer...");
            // Step 1 — stop new records coming in
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for main thread to finish.", e);
                Thread.currentThread().interrupt();
            }
            // Step 2 — stop compactor
            compactor.stop();

            // Step 3 — flush remaining parquet buffers
            archiver.close();

            // Step 4 — close segment files
            try {
                store.close();
            } catch (Exception e) {
                logger.error("Error closing store: ", e);
            }

            logger.info("Shutdown complete.");
        }));
        logger.info("Starting base station consumer loop...");
        consumerLoop.run();

    }

    private static void testBitCask() {
        // 1. Define ONE directory path to use across the whole test
        String testDirectory = "test_bitcask_data";

        try {
            logger.info("--- STARTING BITCASK TEST ---");
            BitCaskStore store = new BitCaskStore(testDirectory);

            // Generate messages (bypassing the 10% null drop for a deterministic test)
            StatusMessage msg1 = synthesizeTestMessage(1L);
            StatusMessage msg2 = synthesizeTestMessage(2L);
            StatusMessage msg1_updated = synthesizeTestMessage(1L);

            logger.info("Writing test data...");
            store.put(1L, msg1);
            store.put(2L, msg2);
            store.put(1L, msg1_updated); // overwrite station 1

            // 2. Use explicit exceptions instead of 'assert'
            if (!store.get(1L).isPresent() || !store.get(1L).get().equals(msg1_updated)) {
                throw new RuntimeException("FAIL: In-memory read did not return updated message!");
            }
            logger.info("In-memory overwrite successful.");

            // Simulate crash
            store.close();

            logger.info("Simulating crash recovery (Store 2)...");
            BitCaskStore store2 = new BitCaskStore(testDirectory); // USE SAME DIRECTORY

            if (store2.get(1L).isEmpty() || store2.get(2L).isEmpty()) {
                store2.close();
                throw new RuntimeException("FAIL: Data did not survive restart!");
            }
            if (!store2.get(1L).get().equals(msg1_updated)) {
                store2.close();
                throw new RuntimeException("FAIL: Recovered data for station 1 was not the latest version!");
            }
            store2.close();

            logger.info("Simulating second crash recovery (Store 3)...");
            BitCaskStore store3 = new BitCaskStore(testDirectory); // USE SAME DIRECTORY
            if (!store3.get(1L).get().equals(msg1_updated)) {
                store3.close();
                throw new RuntimeException("FAIL: Second recovery failed!");
            }
            store3.close();

            logger.info("--- BITCASK TEST PASSED SUCCESSFULLY ---");

        } catch (Exception e) {
            logger.error("Error during BitCask test: ", e);
        }
    }

    // A separate, deterministic generator just for the test to avoid random nulls
    private static StatusMessage synthesizeTestMessage(long stationId) {
        WeatherReading weather = WeatherReading.newBuilder()
                .setHumidity(50)
                .setTemperature(72)
                .setWindSpeed(10).build();

        return StatusMessage.newBuilder()
                .setStationId(stationId)
                .setSNo(sequenceNumber.incrementAndGet())
                .setBatteryStatus("ok")
                .setStatusTimestamp(Instant.now().getEpochSecond())
                .setWeather(weather)
                .build();
    }
}