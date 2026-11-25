package com.weather.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import com.weather.config.AppConfigs;
import com.weather.model.StatusMessage;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class BaseStationConsumer {
    public static KafkaConsumer<Long, StatusMessage> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfigs.getConsumerGroupID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SafeStatusMessageDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfigs.getSchemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, AppConfigs.isSpecificAvroReader());

        return new KafkaConsumer<Long, StatusMessage>(props);
    }

    public static KafkaProducer<Long, byte[]> createDeadLetterProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.getConsumerGroupID() + "-dlq-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

}