package com.weather.my_prod;

import com.weather.config.AppConfigs;
import com.weather.model.StatusMessage;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProducerLoopTest {

    private static Serializer<StatusMessage> noopValueSerializer() {
        return (topic, data) -> new byte[0];
    }

    @Test
    public void testEmittedRecordsUseMonotonicAllocatedStationId() {
        MockProducer<Long, StatusMessage> mockProducer = new MockProducer<>(
                true, new LongSerializer(), noopValueSerializer());
        AtomicLong nextId = new AtomicLong(0);

        ProducerLoop loop = new ProducerLoop(mockProducer, nextId::incrementAndGet);
        loop.run();
        try {
            assertEquals(1L, loop.getStationId());

            for (int i = 0; i < 30 && mockProducer.history().size() < 3; i++) {
                loop.emitOnce(loop.getStationId());
            }

            List<ProducerRecord<Long, StatusMessage>> sent = mockProducer.history();
            assertTrue("Expected at least 3 emitted messages", sent.size() >= 3);
            for (ProducerRecord<Long, StatusMessage> record : sent) {
                assertEquals(AppConfigs.getTopicName(), record.topic());
                assertEquals(Long.valueOf(1L), record.key());
                assertEquals(1L, record.value().getStationId());
            }
        } finally {
            loop.stop();
        }
        assertTrue(mockProducer.closed());
    }

    @Test
    public void testTwoProducerLoopsReceiveMonotonicStationIds() {
        MockProducer<Long, StatusMessage> producer1 = new MockProducer<>(
                true, new LongSerializer(), noopValueSerializer());
        MockProducer<Long, StatusMessage> producer2 = new MockProducer<>(
                true, new LongSerializer(), noopValueSerializer());
        AtomicLong sharedSequence = new AtomicLong(0);

        ProducerLoop loop1 = new ProducerLoop(producer1, sharedSequence::incrementAndGet);
        ProducerLoop loop2 = new ProducerLoop(producer2, sharedSequence::incrementAndGet);
        try {
            loop1.run();
            loop2.run();
            assertEquals(1L, loop1.getStationId());
            assertEquals(2L, loop2.getStationId());
        } finally {
            loop1.stop();
            loop2.stop();
        }
    }
}

