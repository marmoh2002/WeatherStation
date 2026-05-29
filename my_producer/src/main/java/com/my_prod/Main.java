package com.my_prod;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.weather.model.StatusMessage;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<Long, StatusMessage> producer = Producer.createProducer();
        ProducerLoop producerLoop = new ProducerLoop(producer);

        // ── Keep the app alive ───────────────────────────────────────────────
        // we run until the JVM is killed
        // (e.g. docker stop), then clean up gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received — Closing producer (calling producerLoop.stop())...");
            producerLoop.stop();
        }));
        logger.info("Starting producer loop...");
        producerLoop.run();
        // We catch the InterruptedException just in case something tries to wake the
        // main thread up.
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            logger.error("Main thread was interrupted.", e);
        }

    }
}
