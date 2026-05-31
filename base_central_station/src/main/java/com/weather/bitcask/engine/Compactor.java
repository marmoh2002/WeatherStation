package com.weather.bitcask.engine;

import com.weather.bitcask.model.DirEntry;
import com.weather.bitcask.model.HintEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.nio.file.Path;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.weather.bitcask.model.DataEntry;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Compactor {
    private final BitCaskStore store;
    private final ScheduledExecutorService scheduler;
    private final Logger logger = LogManager.getLogger(Compactor.class);

    public Compactor(BitCaskStore st) {
        this.store = st;
        scheduler = Executors.newSingleThreadScheduledExecutor();

    }

    public void start() {
        try {
            logger.info("Starting compactor...");
            scheduler.scheduleAtFixedRate(this::compact, 1, 1, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("Failed to start compactor: {}", e.getMessage(), e);
        }

    }

    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void compact() {
        logger.info("Starting compaction process...");
        try {
            List<SegmentFile> segmentsToCompact = new ArrayList<>(store.getClosedSegments());
            if (segmentsToCompact.size() < 2) {
                logger.info("Not enough segments to compact. Skipping.");
                return;
            }
            // Sort segments by ID (oldest first)
            segmentsToCompact.sort(Comparator.comparingInt(SegmentFile::getSegmentId));
            // Create new segment file for compacted data
            SegmentFile newSegment;
            Map<Long, DataEntry> latestEntries = new HashMap<>();
            Map<Long, Long> writtenOffsets = new HashMap<>();
            try {
                newSegment = new SegmentFile(store.segmentPath(store.getAndIncrementNextSegmentId()), true);
                // Read all entries from segments to compact and keep only the latest for each
                // key
                for (SegmentFile segment : segmentsToCompact) {
                    Map<Long, DataEntry> entries = segment.readAllEntries();
                    for (long offset : entries.keySet()) {
                        DataEntry entry = entries.get(offset);
                        DirEntry dirEntryMem = store.getInMemoryIndex().get(entry.getKey());
                        if (dirEntryMem == null) {
                            continue; // key was deleted after this entry was written
                        }
                        int entrySegmentId = segment.getSegmentId();
                        if (dirEntryMem.getSegmentId() == entrySegmentId && dirEntryMem.getOffset() == offset)
                            latestEntries.put(entry.getKey(), entry);
                    }
                }
                // Write latest entries to new segment file
                for (DataEntry entry : latestEntries.values()) {
                    try {
                        long writtenOffset = newSegment.appendEntry(entry);
                        writtenOffsets.put(entry.getKey(), writtenOffset);

                    } catch (IOException e) {
                        logger.error("Failed to write entry with key {} to new segment: {}", entry.getKey(),
                                e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to create new segment for compaction: {}", e.getMessage(), e);
                return;
            }
            try (HintFile hintFile = new HintFile(store.hintPath(newSegment.getSegmentId()))) {
                for (long e : latestEntries.keySet()) {
                    // the offset returned by appendEntry for this entry
                    hintFile.appendHint(new HintEntry(
                            latestEntries.get(e).getKey(),
                            newSegment.getSegmentId(),
                            writtenOffsets.get(e),
                            latestEntries.get(e).getValue().length));

                }
            }
            // Update store with new segment and remove old segments
            HashMap<Integer, SegmentFile> newSegments = new HashMap<>();
            newSegments.put(newSegment.getSegmentId(), newSegment);
            Set<Integer> oldIds = segmentsToCompact.stream()
                    .map(SegmentFile::getSegmentId)
                    .collect(Collectors.toSet());

            for (long k : latestEntries.keySet()) {
                DirEntry newDirEntry = new DirEntry(newSegment.getSegmentId(), writtenOffsets.get(k),
                        latestEntries.get(k).getValue().length);
                store.updateIndexIfNewer(k, newDirEntry, oldIds);
            }
            store.replaceSegments(oldIds, newSegments);
            newSegment.closeWriter();
            for (Integer oldId : oldIds) {
                Files.deleteIfExists(store.segmentPath(oldId));
                Files.deleteIfExists(store.hintPath(oldId));
            }
            logger.info("Compaction completed. Created new segment {}, removed segments {}",
                    newSegment.getSegmentId(), oldIds);

        } catch (Throwable t) {
            logger.error("Unexpected error during compaction: {}", t.getMessage(), t);
        }
    }

}
