package com.my_stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import com.weather.model.StatusMessage;

import com.weather.config.AppConfigs;

public class WarningApp {

    private static final Logger logger = LogManager.getLogger(WarningApp.class);

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.warningApplicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.getBootstrapServers());
        props.put("schema.registry.url", AppConfigs.getSchemaRegistryUrl());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, StatusMessage> weatherStream = builder.stream(AppConfigs.getWarningInputTopic());
        KStream<Long, StatusMessage> rainingStream = weatherStream.filter((stationId, message) -> {
            try {
                int humidity = message.getWeather().getHumidity();
                boolean isRaining = humidity > AppConfigs.getHumidityThreshold(); // simple threshold for rain

                return isRaining;

            } catch (Exception e) {
                logger.error("Failed to parse message: " + e.getMessage());
                return false; // drop malformed messages
            }
        });

        rainingStream.mapValues(message -> "Rain alert! Station " + message.getStationId()
                + " humidity=" + message.getWeather().getHumidity() + "%")
                .to(AppConfigs.getWarningOutputTopic(), Produced.with(Serdes.Long(), Serdes.String()));

        // builder.build() creates the topology, but doesn't start processing yet
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        // clean shutdown on docker stop
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down streams app...");
            streams.close();
        }));

        logger.info("Starting streams app — listening on: " + AppConfigs.getWarningInputTopic());
        streams.start();

        // keep main thread alive
        Thread.currentThread().join();
    }
}
