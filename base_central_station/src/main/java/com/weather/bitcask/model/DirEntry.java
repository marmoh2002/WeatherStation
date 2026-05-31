package com.weather.bitcask.model;

public class DirEntry {
    private final long segmentId;
    private final int valueSize;
    private final long offset;

    public DirEntry(long segmentId, long offset, int valueSize) {
        this.segmentId = segmentId;
        this.offset = offset;
        this.valueSize = valueSize;
    }

    public Long getSegmentId() {
        return segmentId;
    }

    public int getValueSize() {
        return valueSize;
    }

    public long getOffset() {
        return offset;
    }
}
