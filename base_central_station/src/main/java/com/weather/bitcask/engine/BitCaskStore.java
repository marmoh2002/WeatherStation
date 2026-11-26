package com.weather.bitcask.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.weather.bitcask.model.DataEntry;
import com.weather.bitcask.model.DirEntry;
import com.weather.bitcask.model.HintEntry;
import com.weather.model.StatusMessage;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.nio.file.Path;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Set;
import java.util.ArrayList;
import java.util.Comparator;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.io.Closeable;

public class BitCaskStore implements Closeable {
    private volatile SegmentFile activeSegment;
    private final AtomicLong currentSegmentId = new AtomicLong(0);
    private final Path storeDirectory;
    private final long maxSegmentSizeBytes;
    private final Object writeLock = new Object();
    private final Logger logger = LogManager.getLogger(BitCaskStore.class);
    private final ConcurrentHashMap<Long, SegmentFile> segmentFilesById = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, DirEntry> inMemoryIndex = new ConcurrentHashMap<>(); // stationId →
                                                                                               // (segmentId, offset,
                                                                                               // valueSize)

    public BitCaskStore(String dir) throws Exception {
        this(dir, -1L);
    }

    public BitCaskStore(String dir, long maxSegmentSizeBytes) throws Exception {
        this.storeDirectory = Path.of(dir);
        this.maxSegmentSizeBytes = maxSegmentSizeBytes;
        if (!Files.exists(storeDirectory)) {
            Files.createDirectories(storeDirectory);
            logger.info("Created new store directory at {}", storeDirectory);
        }
        OptionalLong maxId;
        try (Stream<Path> files = Files.list(storeDirectory)) {
            maxId = files
                    .filter(p -> p.toString().endsWith(".data"))
                    .mapToLong(p -> parseSegmentId(p, ".data"))
                    .max();
        }
        if (maxId.isPresent()) {
            currentSegmentId.set(maxId.getAsLong());
            // Load existing segments and build in-memory index
            try {
                recoverFromHintFiles(); // rebuild KeyDir
                logger.info("Recovered in-memory index from hint files with {} entries", inMemoryIndex.size());
            } catch (IOException e) {
                logger.error("Failed to recover from hint files: ", e);
                throw new Exception("Failed to recover from hint files", e);
            }
            // Recover any segments that are missing hint files (e.g. after a crash)
            try {
                recoverSegmentsWithoutHints();
            } catch (IOException e) {
                logger.error("Failed to recover segments without hint files: ", e);
                throw new Exception("Failed to recover segments without hint files", e);
            }
            try {
                reopenSegmentFiles(); // reopen all .data files and set active segment
            } catch (Exception e) {
                logger.error("Failed to initialize BitCaskStore from existing files: ", e);
                throw new Exception("Failed to initialize BitCaskStore from existing files:", e);
            }
        } else {
            long initialId = nextSegmentId();
            activeSegment = new SegmentFile(segmentPath(initialId), true, maxSegmentSizeBytes);
            segmentFilesById.put(initialId, activeSegment);
            logger.info("Initialized new active segment file: {}", activeSegment);
        }
    }

    private static long parseSegmentId(Path path, String suffix) {
        return Long.parseLong(
                path.getFileName().toString().replace(suffix, "").replace("datafile_", ""));
    }

    long nextSegmentId() {
        return currentSegmentId.updateAndGet(prev -> Math.max(Instant.now().toEpochMilli(), prev + 1));
    }

    long getMaxSegmentSizeBytes() {
        return maxSegmentSizeBytes;
    }

    SegmentFile getActiveSegment() {
        return activeSegment;
    }

    public void put(long stationId, StatusMessage message) {

        // → serialize record to bytes
        // → append to active segment file
        // → update inMemoryIndex with new offset

        // Create DataEntry and append to active segment
        byte[] serializedMessage;
        try {
            serializedMessage = AvroSerializer.serialize(message);
        } catch (Exception e) {
            // Handle serialization exception (e.g., log it)
            logger.error("Failed to serialize StatusMessage for stationId " + stationId, e);
            return;
        }

        DataEntry entry = new DataEntry(stationId, serializedMessage);
        synchronized (writeLock) {
            if (activeSegment == null) {
                logger.error("Active segment is not initialized. Cannot put entry for stationId {}", stationId);
                return;
            }
            if (activeSegment.isFull() && !rotateActiveSegment()) {
                return;
            }
            try {
                long offset = activeSegment.appendEntry(entry);
                logger.debug("Appended entry for stationId {} at offset {} in segment with ID {}", stationId, offset,
                        activeSegment.getSegmentId());
                inMemoryIndex.put(stationId,
                        new DirEntry(activeSegment.getSegmentId(), offset, serializedMessage.length));

            } catch (Exception e) {
                // Handle exception
                logger.error("Failed to put entry for stationId {}, segmentId {}", stationId,
                        activeSegment.getSegmentId(), e);
            }
        }
    }

    private boolean rotateActiveSegment() {
        SegmentFile sealedSegment = activeSegment;
        long sealedSegmentId = sealedSegment.getSegmentId();
        SegmentFile newSegment;
        try {
            long nextId = nextSegmentId();
            newSegment = new SegmentFile(segmentPath(nextId), true, maxSegmentSizeBytes);
        } catch (Exception e) {
            logger.error("Failed to create new segment file after segment {}", sealedSegmentId, e);
            return false;
        }

        try {
            sealedSegment.closeWriter();
            logger.info("Sealed segment file: {}", sealedSegment);
        } catch (IOException e) {
            logger.error("Failed to close writer for sealed segment {}: ", sealedSegmentId, e);
        }

        writeHintFileQuietly(sealedSegmentId);

        activeSegment = newSegment;
        segmentFilesById.put(newSegment.getSegmentId(), newSegment);
        logger.info("Created new segment file: {}", activeSegment);
        return true;
    }

    private void writeHintFileQuietly(long segmentId) {
        try (HintFile hintFile = new HintFile(hintPath(segmentId))) {
            logger.info("Created hint file for segment {}: {}", segmentId, hintFile.getSegmentId());
            for (Map.Entry<Long, DirEntry> indexEntry : inMemoryIndex.entrySet()) {
                if (indexEntry.getValue().getSegmentId().equals(segmentId)) {
                    HintEntry hint = new HintEntry(indexEntry.getKey(), segmentId,
                            indexEntry.getValue().getOffset(), indexEntry.getValue().getValueSize());
                    hintFile.appendHint(hint);
                    logger.debug("Appended hint for stationId {} to hint file of segment {}: {}",
                            indexEntry.getKey(), segmentId, hint);
                }
            }
            logger.info("Finished writing hint file for segment {}: {}", segmentId, hintFile.getSegmentId());
        } catch (Exception e) {
            logger.error("Failed to write hint file for segment {}: ", segmentId, e);
        }
    }

    // optional is used to handle the case where the requested stationId does not
    // exist in the index. This way, instead of returning null or throwing an
    // exception, we can return an empty Optional to indicate that the value is not
    // present. If the value is found and successfully retrieved, we return an
    // Optional containing the StatusMessage. This approach promotes better handling
    // of absent values and reduces the likelihood of NullPointerExceptions in the
    // calling code.
    // example usage:
    // StatusMessage message = result.get();

    public Optional<StatusMessage> get(long stationId) {
        DirEntry dirEntry = inMemoryIndex.get(stationId);

        if (dirEntry == null) {
            logger.warn("No entry found in index for stationId {}", stationId);
            return Optional.empty();
        }
        try {
            logger.info("Fetching entry for stationId {} from segmentId {} at offset {}", stationId,
                    dirEntry.getSegmentId(),
                    dirEntry.getOffset());
            SegmentFile targetSegment = segmentFilesById.get(dirEntry.getSegmentId());
            if (targetSegment == null) {
                throw new Exception("Segment file not found for segmentId: " + dirEntry.getSegmentId());
            }

            DataEntry entry = targetSegment.readEntry(dirEntry.getOffset());
            StatusMessage message = AvroSerializer.deserialize(entry.getValue());
            return Optional.of(message);
        } catch (Exception e) {
            logger.error("Failed to get entry for stationId {} from segmentId {}", stationId, dirEntry.getSegmentId(),
                    e);
            return Optional.empty();
        }
    }

    private void recoverFromHintFiles() throws IOException {
        List<Path> hintFiles;
        try (Stream<Path> files = Files.list(storeDirectory)) {
            hintFiles = files.filter(p -> p.toString().endsWith(".hint"))
                    .sorted(Comparator.comparingLong(p -> parseSegmentId(p, ".hint")))
                    .collect(Collectors.toList());
        }
        for (Path hintPath : hintFiles) {
            try {
                List<HintEntry> entries = HintFile.readAll(hintPath);
                for (HintEntry he : entries) {
                    inMemoryIndex.put(he.getKey(),
                            new DirEntry(he.getSegmentId(), he.getOffset(), he.getValueSize()));
                    logger.info(
                            "Recovered hint entry for key {} from hint file {}: segmentId {}, offset {}, valueSize {}",
                            he.getKey(), hintPath, he.getSegmentId(), he.getOffset(), he.getValueSize());
                }
                logger.info("Recovered {} entries from hint file {}", entries.size(), hintPath);
            } catch (IOException e) {
                logger.error("Failed to read hint file {}", hintPath, e);
            }

        }

    }

    /**
     * Recovers segments that do not have a corresponding hint file.
     * This handles non-graceful shutdowns where the active segment's hint file
     * was never written. For each orphaned .data file, the segment is scanned
     * sequentially to rebuild the in-memory index, and a hint file is created.
     */
    private void recoverSegmentsWithoutHints() throws IOException {
        // Collect all .data file paths
        List<Path> dataFiles;
        try (Stream<Path> files = Files.list(storeDirectory)) {
            dataFiles = files
                    .filter(p -> p.toString().endsWith(".data"))
                    .sorted()
                    .collect(Collectors.toList());
        }

        // Collect all existing hint file segment IDs for fast lookup
        Set<Long> hintSegmentIds;
        try (Stream<Path> files = Files.list(storeDirectory)) {
            hintSegmentIds = files
                    .filter(p -> p.toString().endsWith(".hint"))
                    .map(p -> parseSegmentId(p, ".hint"))
                    .collect(Collectors.toSet());
        }

        for (Path dataPath : dataFiles) {
            long segmentId;
            try {
                segmentId = Long.parseLong(
                        dataPath.getFileName().toString().replace(".data", "").replace("datafile_", ""));
            } catch (NumberFormatException e) {
                logger.error("Failed to parse segment ID from file {}", dataPath, e);
                continue;
            }

            if (hintSegmentIds.contains(segmentId)) {
                continue; // hint file already exists, already recovered
            }

            logger.warn("No hint file found for segment {}. Scanning data file to recover entries.", segmentId);

            // Scan the .data file entry by entry to rebuild index and collect hints
            List<HintEntry> recoveredHints = new ArrayList<>();
            long validOffset = 0;
            try (DataInputStream dis = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(dataPath.toFile())))) {
                while (true) {
                    try {
                        int crc = dis.readInt();
                        long timestamp = dis.readLong();
                        long key = dis.readLong();
                        int valueSize = dis.readInt();
                        if (valueSize < 0) {
                            logger.error("Negative valueSize {} at offset {} in {}, truncating.",
                                    valueSize, validOffset, dataPath);
                            break;
                        }
                        byte[] value = new byte[valueSize];
                        dis.readFully(value);

                        DataEntry entry = new DataEntry(Integer.toUnsignedLong(crc), timestamp, key, value);
                        if (!entry.isValid()) {
                            logger.error("CRC32 mismatch at offset {} in {}, truncating corrupted tail.",
                                    validOffset, dataPath);
                            break;
                        }

                        // Update in-memory index (latest value per key wins)
                        inMemoryIndex.put(key, new DirEntry(segmentId, validOffset, valueSize));

                        // Collect hint entry for writing
                        recoveredHints.add(new HintEntry(key, segmentId, validOffset, valueSize));

                        validOffset += SegmentFile.HEADER_SIZE + valueSize;

                        logger.debug("Recovered entry for key {} at offset {} from segment {}",
                                key, validOffset, segmentId);
                    } catch (EOFException eof) {
                        break; // finished reading segment (or torn write at EOF)
                    }
                }
            } catch (IOException e) {
                logger.error("Error scanning segment file {} for recovery", dataPath, e);
                throw e;
            }

            if (validOffset < Files.size(dataPath)) {
                logger.warn("Truncating segment {} from {} to {} bytes due to torn/corrupted trailing record.",
                        dataPath, Files.size(dataPath), validOffset);
                try (RandomAccessFile raf = new RandomAccessFile(dataPath.toFile(), "rw")) {
                    raf.setLength(validOffset);
                }
            }

            // Write the missing hint file
            Path missingHintPath = hintPath(segmentId);
            try (HintFile hf = new HintFile(missingHintPath)) {
                for (HintEntry hint : recoveredHints) {
                    hf.appendHint(hint);
                }
                logger.info("Created missing hint file for segment {} with {} entries",
                        segmentId, recoveredHints.size());
            } catch (IOException e) {
                logger.error("Failed to create hint file for recovered segment {}", segmentId, e);
                throw e;
            }
        }
    }


    private void reopenSegmentFiles() throws Exception {
        List<Path> dataFiles;
        try (Stream<Path> files = Files.list(storeDirectory)) {
            dataFiles = files.filter(p -> p.toString().endsWith(".data")).sorted().collect(Collectors.toList());
        }
        for (Path segPath : dataFiles) {
            try {
                long segmentId = parseSegmentId(segPath, ".data");
                boolean isActive = currentSegmentId.get() == segmentId;
                SegmentFile currSegmentFile = new SegmentFile(segPath, isActive, maxSegmentSizeBytes);
                segmentFilesById.put(segmentId, currSegmentFile);
                if (isActive) {
                    activeSegment = currSegmentFile;
                    logger.info("Set active segment file: {}", activeSegment.getSegmentId());
                }

                logger.info("Reopened segment file: {}", currSegmentFile);

            } catch (NumberFormatException e) {
                logger.error("Failed to parse segment ID from file {}", segPath, e);
            } catch (Exception e) {
                logger.error("Failed to reopen segment file {}", segPath, e);
            }
        }
    }

    // useed to be getAndIncrementNextSegmentId() --- IGNORE ---
    long getCurrentSegmentId() {
        return activeSegment.getSegmentId();
    }

    Path segmentPath(long timestamp) {
        return storeDirectory.resolve(String.format("datafile_%d.data", timestamp));
    }

    Path hintPath(long timestamp) {
        return storeDirectory.resolve(String.format("datafile_%d.hint", timestamp));
    }

    // Give compactor a snapshot of closed segments to work on
    List<SegmentFile> getClosedSegments() {
        synchronized (writeLock) {
            if (activeSegment == null) {
                return new ArrayList<>(segmentFilesById.values());
            }
            long activeId = activeSegment.getSegmentId();
            return segmentFilesById.values().stream()
                    .filter(s -> !s.getSegmentId().equals(activeId))
                    .collect(Collectors.toList());
        }
    }

    // Atomically update inMemoryIndex only if the entry is still currently pointing
    // to the old segment (i.e. no newer writes have happened for that key)
    void updateIndexIfNewer(long key, DirEntry newEntry, Set<Long> oldSegmentIds) {
        inMemoryIndex.computeIfPresent(key, (k, existing) -> {
            // Only replace if still pointing to the old segment
            if (oldSegmentIds.contains(existing.getSegmentId())) {
                return newEntry;
            }
            return existing; // newer write happened, keep it
        });
    }

    void registerSegments(Map<Long, SegmentFile> newSegments) {
        segmentFilesById.putAll(newSegments);
    }

    void removeSegment(long segmentId) {
        segmentFilesById.remove(segmentId);
    }

    // Replace old segments with new compacted ones
    void replaceSegments(Set<Long> oldIds, Map<Long, SegmentFile> newSegments) {
        segmentFilesById.putAll(newSegments);
        for (Long i : oldIds) {
            segmentFilesById.remove(i);
        }
    }

    public ConcurrentHashMap<Long, DirEntry> getInMemoryIndex() {
        return inMemoryIndex;
    }

    @Override
    public void close() {
        synchronized (writeLock) {
            // write hint file before closing
            if (activeSegment != null) {
                writeHintFileQuietly(activeSegment.getSegmentId());
                try {
                    activeSegment.close();
                } catch (IOException e) {
                    logger.error("Error closing active segment: ", e);
                }
            }

            for (SegmentFile segment : segmentFilesById.values()) {
                try {
                    if (segment != activeSegment) {
                        segment.close();
                    }
                    logger.info("Closed segment file {}", segment.getSegmentId());
                } catch (IOException e) {
                    logger.error("Error closing segment {}: ", segment.getSegmentId(), e);
                }
            }
        }
    }

}
