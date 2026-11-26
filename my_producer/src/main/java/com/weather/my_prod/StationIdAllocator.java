package com.weather.my_prod;

import com.weather.config.AppConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Allocates a strictly monotonic station ID across producer containers and restarts
 * by appending a registration record to partition 0 of a single-partition Kafka topic
 * and mapping {@code offset + 1} to the station ID. Honors {@code STATION_ID} if set.
 */
public class StationIdAllocator {
    private static final Logger logger = LogManager.getLogger(StationIdAllocator.class);
    private static final int DEFAULT_MAX_ATTEMPTS = 5;
    private static final long DEFAULT_RETRY_BACKOFF_MS = 1000L;

    @FunctionalInterface
    interface TopicEnsurer {
        void ensureSinglePartitionTopic(String topicName) throws Exception;
    }

    private final String topicName;
    private final Supplier<Long> overrideSupplier;
    private final TopicEnsurer topicEnsurer;
    private final Supplier<Producer<byte[], byte[]>> producerSupplier;
    private final boolean closeProducerAfterUse;
    private final int maxAttempts;
    private final long retryBackoffMs;

    public StationIdAllocator() {
        this(
                AppConfigs.getStationIdTopicName(),
                AppConfigs::getStationIdOverride,
                topic -> ensureTopicViaAdmin(AppConfigs.getBootstrapServers(), topic),
                () -> createCoordinationProducer(AppConfigs.getBootstrapServers()),
                true,
                DEFAULT_MAX_ATTEMPTS,
                DEFAULT_RETRY_BACKOFF_MS);
    }

    StationIdAllocator(
            String topicName,
            Supplier<Long> overrideSupplier,
            TopicEnsurer topicEnsurer,
            Supplier<Producer<byte[], byte[]>> producerSupplier,
            boolean closeProducerAfterUse,
            int maxAttempts,
            long retryBackoffMs) {
        this.topicName = topicName;
        this.overrideSupplier = overrideSupplier;
        this.topicEnsurer = topicEnsurer;
        this.producerSupplier = producerSupplier;
        this.closeProducerAfterUse = closeProducerAfterUse;
        this.maxAttempts = maxAttempts;
        this.retryBackoffMs = retryBackoffMs;
    }

    public static long allocateStationId() {
        return new StationIdAllocator().allocate();
    }

    public long allocate() {
        Long override = overrideSupplier.get();
        if (override != null) {
            logger.info("Using configured STATION_ID override: {}", override);
            return override;
        }

        String instanceTag = UUID.randomUUID().toString();
        Exception lastError = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                topicEnsurer.ensureSinglePartitionTopic(topicName);
                Producer<byte[], byte[]> coordProducer = producerSupplier.get();
                try {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName,
                            0,
                            instanceTag.getBytes(StandardCharsets.UTF_8),
                            Instant.now().toString().getBytes(StandardCharsets.UTF_8));
                    RecordMetadata metadata = coordProducer.send(record).get();
                    long stationId = metadata.offset() + 1L;
                    logger.info("Allocated monotonic station ID {} via topic '{}' (partition {}, offset {})",
                            stationId, topicName, metadata.partition(), metadata.offset());
                    return stationId;
                } finally {
                    if (closeProducerAfterUse && coordProducer != null) {
                        coordProducer.close();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while allocating monotonic station ID", e);
            } catch (Exception e) {
                lastError = e;
                logger.warn("Attempt {}/{} to allocate station ID from topic '{}' failed: {}",
                        attempt, maxAttempts, topicName, e.getMessage());
                if (attempt < maxAttempts && retryBackoffMs > 0) {
                    try {
                        Thread.sleep(retryBackoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while backing off station ID allocation", ie);
                    }
                }
            }
        }

        throw new IllegalStateException(
                "Failed to allocate monotonic station ID from Kafka topic '" + topicName + "' after "
                        + maxAttempts + " attempts",
                lastError);
    }

    private static void ensureTopicViaAdmin(String bootstrapServers, String topicName)
            throws InterruptedException, ExecutionException {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (Admin admin = AdminClient.create(adminProps)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1)
                    .configs(Map.of(
                            TopicConfig.RETENTION_MS_CONFIG, "-1",
                            TopicConfig.RETENTION_BYTES_CONFIG, "-1"));
            try {
                admin.createTopics(Collections.singletonList(newTopic)).all().get();
                logger.info("Created single-partition station ID coordination topic '{}'", topicName);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e;
                }
            }

            TopicDescription description = admin.describeTopics(Collections.singletonList(topicName))
                    .allTopicNames()
                    .get()
                    .get(topicName);
            if (description != null && description.partitions().size() != 1) {
                logger.warn("Coordination topic '{}' has {} partitions; targeting partition 0 for monotonic IDs.",
                        topicName, description.partitions().size());
            }
        }
    }

    private static KafkaProducer<byte[], byte[]> createCoordinationProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.producerApplicationID + "-id-allocator");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaProducer<>(props);
    }
}

