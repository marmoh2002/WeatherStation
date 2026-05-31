package com.weather.bitcask.model;

public class HintEntry {
    private final long key;
    private final int segmentId;
    private final int valueSize;
    private final long offset;

    public HintEntry(Long key, int segmentId, long offset, int valueSize) {
        this.key = key;
        this.segmentId = segmentId;
        this.offset = offset;
        this.valueSize = valueSize;
    }

    public long getKey() {
        return key;
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
