package com.weather;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.weather.archive.engine.ParquetArchiver;
import com.weather.bitcask.engine.BitCaskStore;
import com.weather.bitcask.engine.Compactor;
import com.weather.bitcask.server.BitCaskServer;
import com.weather.config.AppConfigs;
import com.weather.consumer.BaseStationConsumer;
import com.weather.consumer.ConsumerLoop;
import com.weather.model.StatusMessage;
import com.weather.pipeline.Pipeline;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);
    public static void main(String[] args) {
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

        // 4. API Server
        BitCaskServer apiServer = new BitCaskServer(store, 8080);
        apiServer.start();

        // 5. Start consumer — pass pipeline in so poll loop calls pipeline.process()
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

            // Step 3 — stop the API server to release port 8080
            apiServer.stop();

            // Step 4 — flush remaining parquet buffers
            archiver.close();

            // Step 5 — close segment files
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

   }