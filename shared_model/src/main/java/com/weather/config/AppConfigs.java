package com.weather.config;

public class AppConfigs {

    // producer
    public static final String producerApplicationID = "ProducerApp";
    public static final String producerTopicName = "WeatherStationTopic";

    public static String getSchemaRegistryUrl() {
        return System.getenv("SCHEMA_REGISTRY_URL") != null ? System.getenv("SCHEMA_REGISTRY_URL")
                : "http://schema-registry:8081";
    }

    public static String getTopicName() {
        String topic = System.getenv("KAFKA_TOPIC");
        if (topic == null || topic.isEmpty()) {
            return producerTopicName;
        }
        return topic;
    }

    // streams
    public static final String warningApplicationID = "weather-streams";
    public static final String warningOutputTopic = "rain-alerts"; // rainy messages go here
    public static final int humidityThreshold = 70;

    public static String getBootstrapServers() {
        // reads from env variable set in docker-compose, falls back to localhost
        String brokers = System.getenv("KAFKA_BROKER");
        return brokers != null ? brokers : "localhost:9092";
    }

    public static int getHumidityThreshold() {
        // allows overriding the rain threshold via env variable, defaults to 70%
        String threshold = System.getenv("KAFKA_HUMIDITY_THRESHOLD");
        try {
            return threshold != null ? Integer.parseInt(threshold) : humidityThreshold;
        } catch (NumberFormatException e) {
            System.err.println("Invalid KAFKA_HUMIDITY_THRESHOLD value, using default: " + humidityThreshold);
            return humidityThreshold;
        }
    }

    public static String getWarningInputTopic() {
        String topic = System.getenv("KAFKA_WARNING_INPUT_TOPIC");
        if (topic == null || topic.isEmpty()) {
            return producerTopicName;
        }
        return topic;
    }

    public static String getWarningOutputTopic() {
        String topic = System.getenv("KAFKA_WARNING_OUTPUT_TOPIC");
        if (topic == null || topic.isEmpty()) {
            return warningOutputTopic;
        }
        return topic;
    }

}
