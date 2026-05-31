package com.weather.bitcask.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.weather.bitcask.model.*;
import com.weather.config.AppConfigs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;

public class HintFile implements Closeable {
    private final Logger logger = LogManager.getLogger(HintFile.class);
    private final DataOutputStream writer;
    private static final int HINT_FLUSH_THRESHOLD = AppConfigs.getHintFlushThreshold();
    private long offset = 0;
    private int hintsSinceFlush = 0;

    public HintFile(Path filePath) throws IOException {
        logger.info("Creating HintFile at path: {}", filePath);
        try {
            this.writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filePath.toFile(), true))); // append
        } catch (Exception e) {
            logger.error("Error occurred while creating HintFile", e);
            throw new IOException("Failed to create HintFile", e);
        }
    }

    public long appendHint(HintEntry hint) throws IOException {
        try {
            writer.writeLong(hint.getKey());
            writer.writeInt(hint.getSegmentId());
            writer.writeLong(hint.getOffset());
            writer.writeInt(hint.getValueSize());
            offset += Long.BYTES + Integer.BYTES + Long.BYTES + Integer.BYTES; // 24 bytes per hint
            if (++hintsSinceFlush >= HINT_FLUSH_THRESHOLD) {
                writer.flush();
                hintsSinceFlush = 0;
                logger.info("Flushed HintFile after appending {} hints", HINT_FLUSH_THRESHOLD);
            }
            logger.debug("Appended hint for key {} to HintFile at offset {}", hint.getKey(), hint.getOffset());
        } catch (Exception e) {
            logger.error("Error occurred while appending hint to HintFile", e);
            throw new IOException("Failed to append hint to HintFile", e);
        }
        return offset;
    }

    /**
     * READ METHOD (Used only during Startup/Recovery)
     * this is STATIC. no need to instantiate a HintFile object to
     * read.
     * It simply ingests an entire file and returns the list of pointers.
     */
    public static List<HintEntry> readAll(Path filePath) throws IOException {
        List<HintEntry> entries = new ArrayList<>();

        try (DataInputStream reader = new DataInputStream(
                new BufferedInputStream(new FileInputStream(filePath.toFile())))) {

            // While there are still bytes left to read in the file...
            try {
                while (true) {
                    long key = reader.readLong();
                    int segmentId = reader.readInt();
                    long offset = reader.readLong();
                    int valueSize = reader.readInt();

                    entries.add(new HintEntry(key, segmentId, offset, valueSize));
                }
            } catch (EOFException e) {
            }
        }
        return entries;
    }

    @Override
    public void close() throws IOException {
        try {
            writer.flush();
        } catch (IOException e) {
            logger.error("Error occurred while flushing HintFile", e);
            throw e;
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                logger.error("Error occurred while closing HintFile", e);
                throw e;
            }

        }

    }
}
