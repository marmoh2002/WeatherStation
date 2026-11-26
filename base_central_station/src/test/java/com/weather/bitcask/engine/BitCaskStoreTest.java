package com.weather.bitcask.engine;

import com.weather.bitcask.model.DataEntry;
import com.weather.model.StatusMessage;
import com.weather.model.WeatherReading;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BitCaskStoreTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private static StatusMessage makeMessage(long stationId, long sNo, String batteryStatus) {
        return StatusMessage.newBuilder()
                .setStationId(stationId)
                .setSNo(sNo)
                .setBatteryStatus(batteryStatus)
                .setStatusTimestamp(1_700_000_000L + sNo)
                .setWeather(WeatherReading.newBuilder()
                        .setHumidity(45)
                        .setTemperature(22)
                        .setWindSpeed(12)
                        .build())
                .build();
    }

    @Test
    public void testGetReturnsEntriesFromSealedSegmentsAfterFastRotation() throws Exception {
        Path dir = tempDir.newFolder("rotation-store").toPath();
        // Small segment limit forces multiple rotations within < 1 ms
        try (BitCaskStore store = new BitCaskStore(dir.toString(), 96L)) {
            int totalKeys = 12;
            for (long key = 1; key <= totalKeys; key++) {
                store.put(key, makeMessage(key, key * 10, "RECEIVED"));
            }

            List<SegmentFile> closedSegments = store.getClosedSegments();
            assertTrue("Expected multiple sealed segments after rotation", closedSegments.size() >= 3);

            // Verify segment IDs are strictly monotonic and sealed segments keep readers open
            List<Long> allSegmentIds = closedSegments.stream()
                    .map(segment -> segment.getSegmentId())
                    .sorted()
                    .collect(Collectors.toList());
            for (int i = 1; i < allSegmentIds.size(); i++) {
                assertTrue("Segment IDs must be strictly increasing",
                        allSegmentIds.get(i) > allSegmentIds.get(i - 1));
            }
            assertTrue("Active segment ID must exceed all sealed segment IDs",
                    store.getCurrentSegmentId() > allSegmentIds.get(allSegmentIds.size() - 1));

            for (SegmentFile sealed : closedSegments) {
                assertFalse("Sealed segment writer should be closed", sealed.isActive());
            }

            // All entries across sealed segments and active segment must remain readable
            for (long key = 1; key <= totalKeys; key++) {
                Optional<StatusMessage> retrieved = store.get(key);
                assertTrue("Expected key " + key + " to be readable after rotation", retrieved.isPresent());
                assertEquals(key, retrieved.get().getStationId());
                assertEquals(key * 10, retrieved.get().getSNo());
            }

            // Overwrite an older key (forces additional write/rotation) and check both updated and sealed keys
            store.put(1L, makeMessage(1L, 999L, "UPDATED"));
            Optional<StatusMessage> updated = store.get(1L);
            assertTrue(updated.isPresent());
            assertEquals(999L, updated.get().getSNo());
            assertEquals("UPDATED", updated.get().getBatteryStatus().toString());
        }
    }

    @Test
    public void testRotationDoesNotLeaveActiveSegmentNullOrClosedWhenHintCreationFails() throws Exception {
        Path dir = tempDir.newFolder("hint-fail-store").toPath();
        try (BitCaskStore store = new BitCaskStore(dir.toString(), 32L)) {
            // Write first entry so initial active segment becomes full (>= 32 bytes)
            store.put(1L, makeMessage(1L, 1L, "FIRST"));
            SegmentFile firstSegment = store.getActiveSegment();
            assertNotNull(firstSegment);
            assertTrue("First segment should be full after first entry", firstSegment.isFull());

            long sealedId = firstSegment.getSegmentId();
            // Create a directory at the hint path so HintFile creation throws IOException
            Files.createDirectory(store.hintPath(sealedId));

            // Trigger rotation on next put
            store.put(2L, makeMessage(2L, 2L, "SECOND"));

            SegmentFile newActive = store.getActiveSegment();
            assertNotNull("Active segment must not be null after hint failure", newActive);
            long newActiveId = newActive.getSegmentId();
            assertNotEquals("A new active segment must be opened", sealedId, newActiveId);
            assertTrue("New active segment must remain open for writes", newActive.isActive());

            // Both the sealed segment entry and the entry written during failed-hint rotation must be readable
            Optional<StatusMessage> entry1 = store.get(1L);
            Optional<StatusMessage> entry2 = store.get(2L);
            assertTrue(entry1.isPresent());
            assertEquals(1L, entry1.get().getSNo());
            assertTrue(entry2.isPresent());
            assertEquals(2L, entry2.get().getSNo());

            // Subsequent put into the new active segment must also succeed
            store.put(3L, makeMessage(3L, 3L, "THIRD"));
            Optional<StatusMessage> entry3 = store.get(3L);
            assertTrue(entry3.isPresent());
            assertEquals(3L, entry3.get().getSNo());
        }
    }

    @Test
    public void testCompactorCompactsMultipleClosedSegmentsAndPreservesReads() throws Exception {
        Path dir = tempDir.newFolder("compact-store").toPath();
        try (BitCaskStore store = new BitCaskStore(dir.toString(), 96L)) {
            // Write across multiple segments with both unique and overwritten keys
            store.put(1L, makeMessage(1L, 1L, "OLD_1"));
            store.put(2L, makeMessage(2L, 2L, "LIVE_2"));
            store.put(3L, makeMessage(3L, 3L, "OLD_3"));
            store.put(1L, makeMessage(1L, 11L, "LIVE_1"));
            store.put(4L, makeMessage(4L, 4L, "LIVE_4"));
            store.put(5L, makeMessage(5L, 5L, "LIVE_5"));
            store.put(3L, makeMessage(3L, 33L, "LIVE_3"));
            store.put(6L, makeMessage(6L, 6L, "ACTIVE_6"));

            List<SegmentFile> closedBefore = store.getClosedSegments();
            assertTrue("Need at least 2 closed segments for compaction", closedBefore.size() >= 2);
            Set<Long> oldClosedIds = closedBefore.stream()
                    .map(segment -> segment.getSegmentId())
                    .collect(Collectors.toSet());

            Compactor compactor = new Compactor(store);
            compactor.compact();

            List<SegmentFile> closedAfter = store.getClosedSegments();
            assertEquals("Closed segments should be merged into 1 compacted segment", 1, closedAfter.size());
            SegmentFile compactedSegment = closedAfter.get(0);
            assertFalse("Compacted segment writer must be closed", compactedSegment.isActive());
            assertFalse("Compacted segment ID must be new", oldClosedIds.contains(compactedSegment.getSegmentId()));

            // Old .data and .hint files should have been deleted
            for (Long oldId : oldClosedIds) {
                assertFalse("Old .data should be deleted: " + oldId, Files.exists(store.segmentPath(oldId)));
                assertFalse("Old .hint should be deleted: " + oldId, Files.exists(store.hintPath(oldId)));
            }
            assertTrue("New compacted .data should exist", Files.exists(compactedSegment.getFilePath()));
            assertTrue("New compacted .hint should exist", Files.exists(store.hintPath(compactedSegment.getSegmentId())));

            // Reads against compacted segment AND active segment must return latest values
            assertEquals(11L, store.get(1L).orElseThrow().getSNo());
            assertEquals(2L, store.get(2L).orElseThrow().getSNo());
            assertEquals(33L, store.get(3L).orElseThrow().getSNo());
            assertEquals(4L, store.get(4L).orElseThrow().getSNo());
            assertEquals(5L, store.get(5L).orElseThrow().getSNo());
            assertEquals(6L, store.get(6L).orElseThrow().getSNo());
        }

        // Reopen store from disk and verify compacted hint/data recovery works
        try (BitCaskStore reopened = new BitCaskStore(dir.toString(), 96L)) {
            assertEquals(11L, reopened.get(1L).orElseThrow().getSNo());
            assertEquals(2L, reopened.get(2L).orElseThrow().getSNo());
            assertEquals(33L, reopened.get(3L).orElseThrow().getSNo());
            assertEquals(4L, reopened.get(4L).orElseThrow().getSNo());
            assertEquals(5L, reopened.get(5L).orElseThrow().getSNo());
            assertEquals(6L, reopened.get(6L).orElseThrow().getSNo());
        }
    }

    @Test
    public void testCompactorAbortsCleanlyWhenReadFailsBeforeReplace() throws Exception {
        Path dir = tempDir.newFolder("compact-abort-store").toPath();
        try (BitCaskStore store = new BitCaskStore(dir.toString(), 96L)) {
            for (long key = 1; key <= 8; key++) {
                store.put(key, makeMessage(key, key, "MSG_" + key));
            }

            List<SegmentFile> closedBefore = store.getClosedSegments();
            closedBefore.sort(Comparator.comparingLong(segment -> segment.getSegmentId()));
            assertTrue(closedBefore.size() >= 2);

            // Delete the first closed segment's .data file so sequential read throws IOException
            SegmentFile brokenSegment = closedBefore.get(0);
            Files.delete(brokenSegment.getFilePath());

            Set<Long> closedIdsBefore = closedBefore.stream()
                    .map(segment -> segment.getSegmentId())
                    .collect(Collectors.toSet());

            Compactor compactor = new Compactor(store);
            compactor.compact();

            // Old segments in store must remain untouched and no aborted segment file should remain on disk
            Set<Long> closedIdsAfter = store.getClosedSegments().stream()
                    .map(segment -> segment.getSegmentId())
                    .collect(Collectors.toSet());
            assertEquals(closedIdsBefore, closedIdsAfter);

            long dataFileCount = Files.list(dir).filter(p -> p.toString().endsWith(".data")).count();
            // Active + surviving closed segments (minus the 1 deleted file)
            assertEquals(closedBefore.size(), dataFileCount);

            // Intact closed segment entries and active segment entries remain readable
            SegmentFile survivingClosed = closedBefore.get(1);
            assertTrue(survivingClosed.isActive() == false);
            assertTrue("Entry in active segment remains readable", store.get(8L).isPresent());
        }
    }

    @Test
    public void testConcurrentGetAcrossActiveAndSealedSegments() throws Exception {
        Path dir = tempDir.newFolder("concurrent-get-store").toPath();
        int totalKeys = 40;
        Map<Long, StatusMessage> expected = new HashMap<>();

        try (BitCaskStore store = new BitCaskStore(dir.toString(), 96L)) {
            for (long key = 1; key <= totalKeys; key++) {
                StatusMessage msg = makeMessage(key, key * 100L, "BAT_" + key);
                expected.put(key, msg);
                store.put(key, msg);
            }
            // Overwrite a subset of keys so older versions remain in sealed segments
            for (long key = 1; key <= 5; key++) {
                StatusMessage updated = makeMessage(key, key * 1000L + 7L, "UPDATED_" + key);
                expected.put(key, updated);
                store.put(key, updated);
            }

            List<SegmentFile> closedSegments = store.getClosedSegments();
            assertTrue("Expected multiple sealed segments", closedSegments.size() >= 3);

            long activeSegmentId = store.getCurrentSegmentId();
            long sealedKeyCount = store.getInMemoryIndex().values().stream()
                    .filter(dirEntry -> !dirEntry.getSegmentId().equals(activeSegmentId))
                    .count();
            long activeKeyCount = store.getInMemoryIndex().values().stream()
                    .filter(dirEntry -> dirEntry.getSegmentId().equals(activeSegmentId))
                    .count();
            assertTrue("Expected entries in sealed segments", sealedKeyCount > 0);
            assertTrue("Expected entries in active segment", activeKeyCount > 0);

            int threadCount = 16;
            int readsPerThread = 250;
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startGate = new CountDownLatch(1);
            CountDownLatch doneGate = new CountDownLatch(threadCount);
            ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

            for (int t = 0; t < threadCount; t++) {
                final int threadIndex = t;
                pool.submit(() -> {
                    try {
                        startGate.await();
                        for (int i = 0; i < readsPerThread; i++) {
                            long key = ((threadIndex * 13L + i) % totalKeys) + 1L;
                            Optional<StatusMessage> retrieved = store.get(key);
                            if (retrieved.isEmpty()) {
                                fail("Expected key " + key + " to be present on iteration " + i);
                            }
                            StatusMessage actual = retrieved.get();
                            StatusMessage exp = expected.get(key);
                            assertEquals("stationId mismatch for key " + key, exp.getStationId(), actual.getStationId());
                            assertEquals("sNo mismatch for key " + key, exp.getSNo(), actual.getSNo());
                            assertEquals("batteryStatus mismatch for key " + key,
                                    exp.getBatteryStatus().toString(), actual.getBatteryStatus().toString());
                            assertEquals("timestamp mismatch for key " + key,
                                    exp.getStatusTimestamp(), actual.getStatusTimestamp());
                            assertEquals("weather mismatch for key " + key, exp.getWeather(), actual.getWeather());
                        }
                    } catch (Throwable ex) {
                        errors.add(ex);
                    } finally {
                        doneGate.countDown();
                    }
                });
            }

            startGate.countDown();
            assertTrue("Concurrent readers timed out", doneGate.await(30, TimeUnit.SECONDS));
            pool.shutdown();
            assertTrue("Reader pool failed to terminate", pool.awaitTermination(5, TimeUnit.SECONDS));

            if (!errors.isEmpty()) {
                Throwable first = errors.peek();
                throw new AssertionError("Concurrent get() encountered " + errors.size()
                        + " error(s), first: " + first.getMessage(), first);
            }
        }
    }

    @Test
    public void testSegmentFilePartialChannelReadsAndEofHandling() throws Exception {
        byte[] payload = "0123456789ABCDEF".getBytes(StandardCharsets.UTF_8);
        try (ChunkedReadOnlyFileChannel channel = new ChunkedReadOnlyFileChannel(payload, 3)) {
            ByteBuffer fullRead = ByteBuffer.allocate(10);
            SegmentFile.readFully(channel, fullRead, 2);
            fullRead.flip();
            byte[] actual = new byte[10];
            fullRead.get(actual);
            assertArrayEquals("23456789AB".getBytes(StandardCharsets.UTF_8), actual);

            ByteBuffer pastEof = ByteBuffer.allocate(6);
            boolean threwEof = false;
            try {
                SegmentFile.readFully(channel, pastEof, payload.length - 2);
            } catch (EOFException expected) {
                threwEof = true;
            }
            assertTrue("Expected EOFException when channel reaches -1 before buffer fills", threwEof);
        }

        Path segPath = tempDir.newFile("datafile_1.data").toPath();
        byte[] value = AvroSerializer.serialize(makeMessage(77L, 5L, "OK"));
        try (SegmentFile segment = new SegmentFile(segPath, true, 1024L)) {
            long offset = segment.appendEntry(new DataEntry(77L, value));
            DataEntry entry = segment.readEntry(offset);
            assertEquals(Long.valueOf(77L), entry.getKey());
            assertArrayEquals(value, entry.getValue());

            segment.closeReader();
            boolean threwOnClosedReader = false;
            try {
                segment.readEntry(offset);
            } catch (IOException expected) {
                threwOnClosedReader = true;
            }
            assertTrue("Expected IOException after closeReader()", threwOnClosedReader);
        }
    }

    private static final class ChunkedReadOnlyFileChannel extends FileChannel {
        private final byte[] data;
        private final int maxChunkBytes;

        private ChunkedReadOnlyFileChannel(byte[] data, int maxChunkBytes) {
            this.data = data;
            this.maxChunkBytes = maxChunkBytes;
        }

        @Override
        public int read(ByteBuffer dst, long position) {
            if (position >= data.length) {
                return -1;
            }
            int available = (int) Math.min(data.length - position, Math.min(maxChunkBytes, dst.remaining()));
            dst.put(data, (int) position, available);
            return available;
        }

        @Override
        protected void implCloseChannel() {
        }

        @Override public int read(ByteBuffer dst) { throw new UnsupportedOperationException(); }
        @Override public long read(ByteBuffer[] dsts, int offset, int length) { throw new UnsupportedOperationException(); }
        @Override public int write(ByteBuffer src) { throw new UnsupportedOperationException(); }
        @Override public long write(ByteBuffer[] srcs, int offset, int length) { throw new UnsupportedOperationException(); }
        @Override public long position() { throw new UnsupportedOperationException(); }
        @Override public FileChannel position(long newPosition) { throw new UnsupportedOperationException(); }
        @Override public long size() { return data.length; }
        @Override public FileChannel truncate(long size) { throw new UnsupportedOperationException(); }
        @Override public void force(boolean metaData) { throw new UnsupportedOperationException(); }
        @Override public long transferTo(long position, long count, WritableByteChannel target) { throw new UnsupportedOperationException(); }
        @Override public long transferFrom(ReadableByteChannel src, long position, long count) { throw new UnsupportedOperationException(); }
        @Override public int write(ByteBuffer src, long position) { throw new UnsupportedOperationException(); }
        @Override public MappedByteBuffer map(MapMode mode, long position, long size) { throw new UnsupportedOperationException(); }
        @Override public FileLock lock(long position, long size, boolean shared) { throw new UnsupportedOperationException(); }
        @Override public FileLock tryLock(long position, long size, boolean shared) { throw new UnsupportedOperationException(); }
    }
}
