package com.weather.bitcask.engine;

import com.weather.bitcask.model.DirEntry;
import com.weather.bitcask.model.HintEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.io.DataInputStream;
import java.io.EOFException;
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
import com.weather.config.AppConfigs;

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
            scheduler.scheduleAtFixedRate(this::compact, 1, AppConfigs.getCompactionInterval(), TimeUnit.MINUTES);
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

    public synchronized void compact() {
        logger.info("Starting compaction process...");

        List<SegmentFile> segmentsToCompact = new ArrayList<>(store.getClosedSegments());
        if (segmentsToCompact.size() < 2) {
            logger.info("Not enough segments to compact. Skipping.");
            return;
        }
        // Sort segments by ID (oldest first)
        segmentsToCompact.sort(Comparator.comparingLong(segment -> segment.getSegmentId()));
        // Create new segment file for compacted data
        SegmentFile newSegment = null;
        boolean replaced = false;

        Set<Long> oldIds = segmentsToCompact.stream()
                .map(segment -> segment.getSegmentId())
                .collect(Collectors.toSet());

        Map<Long, Long> writtenOffsets = new HashMap<>();

        Map<Long, Integer> writtenValueSizes = new HashMap<>();

        try {
            long newSegmentId = store.nextSegmentId();
            newSegment = new SegmentFile(store.segmentPath(newSegmentId), true, store.getMaxSegmentSizeBytes());
            // Read all entries from segments to compact and keep only the latest for each
            // key
            for (SegmentFile segment : segmentsToCompact) {
                long segmentId = segment.getSegmentId();

                try (DataInputStream in = segment.openForSequentialRead()) {
                    long offset = 0;

                    while (true) {
                        int crc;
                        long timestamp;
                        long key;
                        int valueSize;
                        try {
                            crc = in.readInt();
                            timestamp = in.readLong();
                            key = in.readLong();
                            valueSize = in.readInt();
                        } catch (EOFException e) {
                            break; // clean end of segment
                        }
                        if (valueSize < 0) {
                            logger.error("Corrupted valueSize {} at offset {} in segment {}, stopping compaction read.",
                                    valueSize, offset, segmentId);
                            break;
                        }
                        long recordOffset = offset;
                        offset += SegmentFile.HEADER_SIZE + valueSize;

                        // Is this record the current live version?
                        DirEntry live = store.getInMemoryIndex().get(key);
                        boolean isLive = live != null
                                && live.getSegmentId().equals(segmentId)
                                && live.getOffset() == recordOffset;

                        if (!isLive) {
                            // Skip: read past the value bytes without allocating
                            long remaining = valueSize;
                            while (remaining > 0) {
                                long skipped = in.skip(remaining);
                                if (skipped <= 0)
                                    throw new EOFException("Unexpected end during skip");
                                remaining -= skipped;
                            }
                            continue;
                        }

                        // Live record: read value, verify CRC32, and immediately stream to new segment
                        byte[] value = new byte[valueSize];
                        in.readFully(value);

                        DataEntry entry = new DataEntry(Integer.toUnsignedLong(crc), timestamp, key, value);
                        if (!entry.isValid()) {
                            logger.error("Corrupted live record at offset {} in segment {} (CRC32 mismatch), skipping.",
                                    recordOffset, segmentId);
                            continue;
                        }

                        long newOffset = newSegment.appendEntry(new DataEntry(timestamp, key, value));
                        writtenOffsets.put(key, newOffset);
                        writtenValueSizes.put(key, valueSize);
                    }

                } catch (IOException e) {
                    logger.error("Error streaming segment {} during compaction", segmentId, e);
                    return; // abort; old segments untouched
                }
            }
            // Write hint file for the new compacted segment
            try (HintFile hintFile = new HintFile(store.hintPath(newSegmentId))) {
                for (long key : writtenOffsets.keySet()) {
                    hintFile.appendHint(new HintEntry(
                            key,
                            newSegmentId,
                            writtenOffsets.get(key),
                            writtenValueSizes.get(key)));
                }
            }
            // Atomically update index and swap segments
            Map<Long, SegmentFile> newSegments = new HashMap<>();
            newSegments.put(newSegmentId, newSegment);

            store.registerSegments(newSegments);

            for (long key : writtenOffsets.keySet()) {
                DirEntry newEntry = new DirEntry(
                        newSegmentId,
                        writtenOffsets.get(key),
                        writtenValueSizes.get(key));
                store.updateIndexIfNewer(key, newEntry, oldIds);
            }

            store.replaceSegments(oldIds, newSegments);
            replaced = true;

            try {
                newSegment.closeWriter();
                logger.info("Closed writer for new compacted segment file: {}", newSegmentId);
            } catch (IOException e) {
                logger.error("Error closing writer for new compacted segment {}", newSegmentId, e);
            }

            for (SegmentFile oldSegment : segmentsToCompact) {
                try {
                    oldSegment.close();
                } catch (IOException e) {
                    logger.error("Error closing compacted old segment {}", oldSegment.getSegmentId(), e);
                }
            }

            for (Long oldId : oldIds) {
                Files.deleteIfExists(store.segmentPath(oldId));
                Files.deleteIfExists(store.hintPath(oldId));
            }

            logger.info("Compaction done. New segment: {}, removed: {}",
                    newSegmentId, oldIds);

        } catch (Throwable t) {
            logger.error("Unexpected error during compaction", t);
        } finally {
            if (newSegment != null && !replaced) {
                try {
                    store.removeSegment(newSegment.getSegmentId());
                    newSegment.close();
                    Files.deleteIfExists(newSegment.getFilePath());
                    Files.deleteIfExists(store.hintPath(newSegment.getSegmentId()));
                } catch (IOException e) {
                    logger.error("Error closing new segment during compaction cleanup", e);
                }
            }
        }

    }

}