package com.weather.bitcask.model;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32;
import java.time.Instant;

public class DataEntry {
    private final long crc32;
    private final long timestamp;
    private final Long key;
    private final int valueSize;
    private final byte[] value;

    // Constructor for creating new records to write (sets timestamp automatically)
    public DataEntry(Long key, byte[] value) {
        this(Instant.now().getEpochSecond(), key, value);
    }

    public DataEntry(long timestamp, Long key, byte[] value) {
        this.timestamp = timestamp;
        this.key = key;
        this.valueSize = value.length;
        this.value = value;
        this.crc32 = computeCrc32(this.timestamp, this.key, this.value);
    }

    // Constructor for reading existing records from disk
    public DataEntry(long crc32, long timestamp, Long key, byte[] value) {
        this.crc32 = crc32;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.valueSize = (value != null) ? value.length : 0;
    }

    public static long computeCrc32(long timestamp, Long key, byte[] value) {
        CRC32 crc = new CRC32();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Long.BYTES + Integer.BYTES);
        buffer.putLong(timestamp);
        buffer.putLong(key != null ? key : 0L);
        buffer.putInt(value != null ? value.length : 0);
        crc.update(buffer.array());
        if (value != null && value.length > 0) {
            crc.update(value);
        }
        return crc.getValue();
    }

    public boolean isValid() {
        return this.crc32 == computeCrc32(this.timestamp, this.key, this.value);
    }

    public long getCrc32() {
        return crc32;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Long getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public Integer getValueSize() {
        return valueSize;
    }

    @Override
    public int hashCode() {
        // hash code based on key+valueSize+value content
        return Objects.hash(crc32, timestamp, key, valueSize, Arrays.hashCode(value));
    }

}
