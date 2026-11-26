package com.weather.my_prod;

import com.weather.config.AppConfigs;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StationIdAllocatorTest {

    @Test
    public void testAllocatesStrictlyMonotonicIdsAcrossSimulatedContainersAndRestarts() {
        MockProducer<byte[], byte[]> sharedBrokerLog = new MockProducer<>(
                true, new ByteArraySerializer(), new ByteArraySerializer());

        List<Long> allocatedIds = new ArrayList<>();
        for (int container = 0; container < 10; container++) {
            StationIdAllocator allocator = new StationIdAllocator(
                    "weather-station-ids",
                    () -> null,
                    topic -> {
                    },
                    () -> sharedBrokerLog,
                    false,
                    3,
                    0L);
            allocatedIds.add(allocator.allocate());
        }

        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), allocatedIds);

        // Every registration record must target partition 0 of the coordination topic
        for (ProducerRecord<byte[], byte[]> record : sharedBrokerLog.history()) {
            assertEquals("weather-station-ids", record.topic());
            assertEquals(Integer.valueOf(0), record.partition());
        }
    }

    @Test
    public void testConcurrentAllocationProducesUniqueSequentialIds() throws Exception {
        MockProducer<byte[], byte[]> sharedBrokerLog = new MockProducer<>(
                true, new ByteArraySerializer(), new ByteArraySerializer());

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Callable<Long>> tasks = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                tasks.add(() -> new StationIdAllocator(
                        "weather-station-ids",
                        () -> null,
                        topic -> {
                        },
                        () -> sharedBrokerLog,
                        false,
                        3,
                        0L).allocate());
            }

            List<Future<Long>> futures = pool.invokeAll(tasks);
            List<Long> results = new ArrayList<>();
            for (Future<Long> future : futures) {
                results.add(future.get());
            }
            Collections.sort(results);
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L), results);
            Set<Long> unique = new HashSet<>(results);
            assertEquals(threads, unique.size());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testStationIdOverrideBypassesKafkaCoordination() {
        AtomicInteger kafkaCalls = new AtomicInteger(0);
        StationIdAllocator allocator = new StationIdAllocator(
                "weather-station-ids",
                () -> AppConfigs.parseStationIdOverride("42"),
                topic -> {
                    kafkaCalls.incrementAndGet();
                    throw new AssertionError("Topic ensurer should not be called when STATION_ID is set");
                },
                () -> {
                    kafkaCalls.incrementAndGet();
                    throw new AssertionError("Kafka producer should not be called when STATION_ID is set");
                },
                false,
                3,
                0L);

        assertEquals(42L, allocator.allocate());
        assertEquals(0, kafkaCalls.get());
    }

    @Test
    public void testParseStationIdOverrideValidation() {
        assertEquals(Long.valueOf(0L), AppConfigs.parseStationIdOverride("0"));
        assertEquals(Long.valueOf(15L), AppConfigs.parseStationIdOverride(" 15 "));
        assertNull(AppConfigs.parseStationIdOverride(null));
        assertNull(AppConfigs.parseStationIdOverride(""));
        assertNull(AppConfigs.parseStationIdOverride("  "));
        assertNull(AppConfigs.parseStationIdOverride("-1"));
        assertNull(AppConfigs.parseStationIdOverride("not-a-number"));
    }

    @Test
    public void testRetriesTransientTopicSetupFailuresAndThrowsWhenExhausted() {
        MockProducer<byte[], byte[]> sharedBrokerLog = new MockProducer<>(
                true, new ByteArraySerializer(), new ByteArraySerializer());
        AtomicInteger attempts = new AtomicInteger(0);

        StationIdAllocator recoveringAllocator = new StationIdAllocator(
                "weather-station-ids",
                () -> null,
                topic -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new RuntimeException("transient metadata error");
                    }
                },
                () -> sharedBrokerLog,
                false,
                3,
                0L);

        assertEquals(1L, recoveringAllocator.allocate());
        assertEquals(3, attempts.get());

        StationIdAllocator failingAllocator = new StationIdAllocator(
                "weather-station-ids",
                () -> null,
                topic -> {
                    throw new RuntimeException("broker down");
                },
                () -> sharedBrokerLog,
                false,
                2,
                0L);

        try {
            failingAllocator.allocate();
            fail("Expected IllegalStateException when all retries fail");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("weather-station-ids"));
        }
    }
}

