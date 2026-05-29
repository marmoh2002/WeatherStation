package com.weather;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.weather.model.StatusMessage;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        KafkaConsumer<Long, StatusMessage> consumer = BaseStationConsumer.createConsumer();

        ConsumerLoop consumerLoop = new ConsumerLoop(consumer);
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received. Waking up consumer...");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for main thread to finish.", e);
            }
        }));
        logger.info("Starting base station consumer loop...");
        consumerLoop.run();

    }
}