package com.weather.bitcask.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.weather.bitcask.model.*;
import com.weather.config.AppConfigs;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;

public class SegmentFile implements Closeable {
    private final Path filePath;
    private long currentOffset = 0;
    private final DataOutputStream writer;
    private final RandomAccessFile reader;
    private boolean isActive = false;
    private static final int FLUSH_THRESHOLD = AppConfigs.getDiskFlushThreshold();
    private int writesSinceFlush = 0;
    private static final Logger logger = LogManager.getLogger(SegmentFile.class);

    public SegmentFile(Path filePath, boolean isActive) throws Exception {
        logger.info("Creating SegmentFile at path: {}, isActive: {}", filePath, isActive);
        try {
            this.filePath = filePath;
            this.writer = isActive ? new DataOutputStream(new FileOutputStream(filePath.toFile(), true)) : null; // append
            this.currentOffset = filePath.toFile().length();
            this.reader = new RandomAccessFile(filePath.toFile(), "r");
            this.isActive = isActive;

        } catch (Exception e) {
            logger.error("Error occurred while creating SegmentFile", e);
            throw new Exception("Failed to create SegmentFile", e);
        }
    }

    public long appendEntry(DataEntry entry) throws IOException {
        // Serialize the entry and write to the file
        if (writer == null) {
            throw new IllegalStateException("Cannot write to a closed segment");
        }
        long entryOffset = currentOffset;
        writer.writeLong(entry.getKey()); // write key
        writer.writeInt(entry.getValueSize()); // write value size
        writer.write(entry.getValue()); // write entry data

        if (++writesSinceFlush >= FLUSH_THRESHOLD) {
            writer.flush();
            writesSinceFlush = 0;
        }
        currentOffset += Long.BYTES + Integer.BYTES + entry.getValueSize(); // update current offset
        return entryOffset;
    }

    public DataEntry readEntry(long offset) throws IOException {
        synchronized (this) {
            reader.seek(offset);
            long key = reader.readLong();
            int valueSize = reader.readInt();
            byte[] value = new byte[valueSize];
            reader.readFully(value);
            return new DataEntry(key, value);
        }
    }

    public Map<Long, DataEntry> readAllEntries() {
        Map<Long, DataEntry> entries = new HashMap<>();
        try (DataInputStream reader = new DataInputStream(
                new BufferedInputStream(new FileInputStream(filePath.toFile())))) {
            try {
                long offset = 0;
                while (true) {
                    long key = reader.readLong();
                    int valueSize = reader.readInt();
                    byte[] value = new byte[valueSize];
                    reader.readFully(value);
                    entries.put(offset, new DataEntry(key, value));
                    offset += Long.BYTES + Integer.BYTES + valueSize;
                    logger.debug("Read entry with key {} from offset {}", key, offset);
                }
            } catch (EOFException e) {
                logger.info("Finished reading all entries from segment file: {}", filePath);
            }
        } catch (IOException e) {
            logger.error("Error reading entries from segment file: {}", filePath, e);
        }
        return entries;
    }

    public Long getSegmentId() {
        String fileName = filePath.getFileName().toString();
        try {
            return Long.parseLong(fileName.replace(".data", "").replace("datafile_", ""));
        } catch (NumberFormatException e) {
            logger.error("Failed to parse segment ID from file name: {}", fileName, e);
            throw new IllegalStateException("Invalid segment file name: " + fileName, e);
        }

    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public Path getFilePath() {
        return filePath;
    }

    public boolean isFull() {
        return currentOffset >= AppConfigs.getMaxSegmentSize();
    }

    public boolean isActive() {
        return isActive;
    }

    public void closeWriter() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
        // reader stays open
    }

    public void closeReader() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public void close() throws IOException {
        closeWriter();
        closeReader();
    }

}
