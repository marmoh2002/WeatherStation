package com.weather.my_stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Properties;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import com.weather.model.StatusMessage;

import com.weather.config.AppConfigs;

public class WarningApp {

    private static final Logger logger = LogManager.getLogger(WarningApp.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

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
                if (message == null || message.getWeather() == null) {
                    return false; // ignore messages without weather data
                }
                int humidity = message.getWeather().getHumidity();
                boolean isRaining = humidity > AppConfigs.getHumidityThreshold(); // simple threshold for rain

                return isRaining;

            } catch (Exception e) {
                logger.error("Failed to parse message: " + e.getMessage());
                return false; // drop malformed messages
            }
        });

        rainingStream.mapValues(message -> {
            try {
                // Build the structured JSON object
                ObjectNode alert = objectMapper.createObjectNode();
                alert.put("station_id", message.getStationId());
                alert.put("status_timestamp", message.getStatusTimestamp());
                alert.put("humidity", message.getWeather().getHumidity());
                alert.put("alert_type", "RAIN_TRIGGER");
                alert.put("message",
                        "Heavy precipitation alert: humidity exceeds " + AppConfigs.getHumidityThreshold() + "%");
                // Convert JSON object to String
                return objectMapper.writeValueAsString(alert);
            } catch (Exception e) {
                logger.error("Failed to serialize rain alert: " + e.getMessage());
                return "{}";
            }
        }).to(AppConfigs.getWarningOutputTopic(), Produced.with(Serdes.Long(), Serdes.String()));

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
