package com.weather.bitcask.engine;

import com.weather.model.StatusMessage;
import com.weather.model.WeatherReading;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
                    .map(SegmentFile::getSegmentId)
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
                    .map(SegmentFile::getSegmentId)
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
            closedBefore.sort(Comparator.comparingLong(SegmentFile::getSegmentId));
            assertTrue(closedBefore.size() >= 2);

            // Delete the first closed segment's .data file so sequential read throws IOException
            SegmentFile brokenSegment = closedBefore.get(0);
            Files.delete(brokenSegment.getFilePath());

            Set<Long> closedIdsBefore = closedBefore.stream()
                    .map(SegmentFile::getSegmentId)
                    .collect(Collectors.toSet());

            Compactor compactor = new Compactor(store);
            compactor.compact();

            // Old segments in store must remain untouched and no aborted segment file should remain on disk
            Set<Long> closedIdsAfter = store.getClosedSegments().stream()
                    .map(SegmentFile::getSegmentId)
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
}
