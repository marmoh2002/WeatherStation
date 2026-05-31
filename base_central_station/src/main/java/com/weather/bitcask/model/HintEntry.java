package com.weather.bitcask.model;

public class HintEntry {
    private final long key;
    private final long segmentId;
    private final int valueSize;
    private final long offset;

    public HintEntry(Long key, long segmentId, long offset, int valueSize) {
        this.key = key;
        this.segmentId = segmentId;
        this.offset = offset;
        this.valueSize = valueSize;
    }

    public long getKey() {
        return key;
    }

    public long getSegmentId() {
        return segmentId;
    }

    public int getValueSize() {
        return valueSize;
    }

    public long getOffset() {
        return offset;
    }

}
