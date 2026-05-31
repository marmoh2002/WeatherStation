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
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.nio.file.Path;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;
import java.io.Closeable;

public class BitCaskStore implements Closeable {
    private SegmentFile activeSegment;
    private HintFile activeHintFile;
    private final AtomicInteger nextSegmentId = new AtomicInteger(0);
    private final Path storeDirectory;
    private final Logger logger = LogManager.getLogger(BitCaskStore.class);
    private final ConcurrentHashMap<Integer, SegmentFile> segmentFilesById = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, DirEntry> inMemoryIndex = new ConcurrentHashMap<>(); // stationId →
                                                                                               // (segmentId, offset,
                                                                                               // valueSize)

    public BitCaskStore(String dir) throws Exception {
        storeDirectory = Path.of(dir);
        if (!Files.exists(storeDirectory)) {
            Files.createDirectories(storeDirectory);
            logger.info("Created new store directory at {}", storeDirectory);
        }
        OptionalInt maxId = Files.list(storeDirectory)
                .filter(p -> p.toString().endsWith(".data"))
                .mapToInt(p -> Integer.parseInt(
                        p.getFileName().toString().replace(".data", "")))
                .max();
        if (maxId.isPresent()) {
            nextSegmentId.set(maxId.getAsInt() + 1);
            // Load existing segments and build in-memory index
            try {
                recoverFromHintFiles(); // rebuild KeyDir
                logger.info("Recovered in-memory index from hint files with {} entries", inMemoryIndex.size());
            } catch (IOException e) {
                logger.error("Failed to recover from hint files: ", e);
                throw new Exception("Failed to recover from hint files", e);
            }
            try {
                reopenSegmentFiles(); // reopen all .data files and set active segment
            } catch (Exception e) {
                logger.error("Failed to initialize BitCaskStore from existing files: ", e);
                throw new Exception("Failed to initialize BitCaskStore from existing files:", e);
            }
        } else {
            activeSegment = new SegmentFile(segmentPath(nextSegmentId.getAndIncrement()), true);
            segmentFilesById.put(activeSegment.getSegmentId(), activeSegment);
            logger.info("Initialized new active segment file: {}", activeSegment);
        }
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
        if (activeSegment == null) {
            logger.error("Active segment is not initialized. Cannot put entry for stationId {}", stationId);
            return;
        }
        if (activeSegment.isFull()) {
            try {
                int sealedSegmentId = activeSegment.getSegmentId();
                activeSegment.close();
                logger.info("Sealed segment file: {}", activeSegment);
                try {
                    activeHintFile = new HintFile(hintPath(sealedSegmentId));
                    logger.info("Created hint file for sealed segment {}: {}", activeSegment, activeHintFile);
                } catch (Exception e) {
                    logger.error("Failed to create hint file for sealed segment {}: ", activeSegment, e);
                }
                try {
                    for (Map.Entry<Long, DirEntry> indexEntry : inMemoryIndex.entrySet()) {
                        if (indexEntry.getValue().getSegmentId() == activeSegment.getSegmentId()) {
                            HintEntry hint = new HintEntry(indexEntry.getKey(), activeSegment.getSegmentId(),
                                    indexEntry.getValue().getOffset(), indexEntry.getValue().getValueSize());
                            activeHintFile.appendHint(hint);
                            logger.debug("Appended hint for stationId {} to hint file of segment {}: {}",
                                    indexEntry.getKey(),
                                    activeSegment, hint);
                        }
                    }
                    activeHintFile.close();
                    logger.info("Finished writing hint file for sealed segment {}: {}", activeSegment, activeHintFile);
                } catch (Exception e) {
                    logger.error("Failed to write hint file for sealed segment {}: ", activeSegment, e);
                }
                activeSegment = new SegmentFile(segmentPath(nextSegmentId.getAndIncrement()), true);
                segmentFilesById.put(activeSegment.getSegmentId(), activeSegment);
                logger.info("Created new segment file: {}", activeSegment);
            } catch (Exception e) {
                logger.error("Failed to create new segment file for stationId " + stationId, e);
                return;
            }
        }
        try {

            long offset = activeSegment.appendEntry(entry);
            logger.debug("Appended entry for stationId {} at offset {} in segment {}", stationId, offset,
                    activeSegment);
            inMemoryIndex.put(stationId,
                    new DirEntry(nextSegmentId.get() - 1, offset, serializedMessage.length));

        } catch (Exception e) {
            // Handle exception
            logger.error("Failed to put entry for stationId {}, segmentId {}", stationId, nextSegmentId.get() - 1, e);
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
        List<Path> hintFiles = Files.list(storeDirectory).filter(p -> p.toString().endsWith(".hint")).sorted()
                .collect(Collectors.toList());
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

    private void reopenSegmentFiles() throws Exception {
        Files.list(storeDirectory).filter(p -> p.toString().endsWith(".data")).sorted().forEach(segPath -> {
            try {
                int segmentId = Integer.parseInt(segPath.getFileName().toString().replace(".data", ""));
                boolean isActive = nextSegmentId.get() == (segmentId + 1); // active segment is the one with the highest
                                                                           // ID
                SegmentFile currSegmentFile = new SegmentFile(segPath, isActive);
                segmentFilesById.put(segmentId, currSegmentFile);
                if (isActive) {
                    activeSegment = currSegmentFile;
                }
                logger.info("Reopened segment file: {}", currSegmentFile);

            } catch (NumberFormatException e) {
                logger.error("Failed to parse segment ID from file {}", segPath, e);
            } catch (Exception e) {
                logger.error("Failed to reopen segment file {}", segPath, e);
            }

        });

    }

    int getAndIncrementNextSegmentId() {
        return nextSegmentId.getAndIncrement();
    }

    Path segmentPath(int id) {
        return storeDirectory.resolve(String.format("%09d.data", id));
    }

    Path hintPath(int id) {
        return storeDirectory.resolve(String.format("%09d.hint", id));
    }

    // Give compactor a snapshot of closed segments to work on
    List<SegmentFile> getClosedSegments() {
        return segmentFilesById.values().stream()
                .filter(s -> s.getSegmentId() != activeSegment.getSegmentId())
                .collect(Collectors.toList());
    }

    // Atomically update inMemoryIndex only if the entry is still current
    void updateIndexIfNewer(long key, DirEntry newEntry, Set<Integer> oldSegmentIds) {
        inMemoryIndex.computeIfPresent(key, (k, existing) -> {
            // Only replace if still pointing to the old segment
            if (oldSegmentIds.contains(existing.getSegmentId())) {
                return newEntry;
            }
            return existing; // newer write happened, keep it
        });
    }

    // Replace old segments with new compacted ones
    void replaceSegments(Set<Integer> oldIds, Map<Integer, SegmentFile> newSegments) {
        for (Integer i : oldIds) {
            segmentFilesById.remove(i);
        }
        segmentFilesById.putAll(newSegments);
    }

    ConcurrentHashMap<Long, DirEntry> getInMemoryIndex() {
        return inMemoryIndex;
    }

    @Override
    public void close() {
        // write hint file before closing
        try {
            int id = activeSegment.getSegmentId();
            activeHintFile = new HintFile(hintPath(id));
            for (Map.Entry<Long, DirEntry> e : inMemoryIndex.entrySet()) {
                if (e.getValue().getSegmentId() == id) {
                    activeHintFile.appendHint(new HintEntry(
                            e.getKey(),
                            e.getValue().getSegmentId(),
                            e.getValue().getOffset(),
                            e.getValue().getValueSize()));
                }
            }
            activeHintFile.close();
            logger.info("Wrote hint file for active segment {} on close", id);
        } catch (Exception e) {
            logger.error("Failed to write hint file on close", e);
        }
        try {
            activeSegment.close();

        } catch (IOException e) {
            logger.error("Error closing active segment: ", e);
        }

        for (SegmentFile segment : segmentFilesById.values()) {
            try {
                if (segment != activeSegment)
                    segment.close();
                logger.info("Closed segment file {}", segment.getSegmentId());
            } catch (IOException e) {
                logger.error("Error closing segment {}: ", segment.getSegmentId(), e);
            }
        }
    }

}
