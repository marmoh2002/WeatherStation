package com.weather.bitcask.model;

public class DirEntry {
    private final int segmentId;
    private final int valueSize;
    private final long offset;

    public DirEntry(int segmentId, long offset, int valueSize) {
        this.segmentId = segmentId;
        this.offset = offset;
        this.valueSize = valueSize;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public int getValueSize() {
        return valueSize;
    }

    public long getOffset() {
        return offset;
    }
}
