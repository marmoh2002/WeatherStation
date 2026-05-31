package com.weather.bitcask.model;

import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.h;
import java.util.Arrays;
import java.util.Objects;

public class DataEntry {
    private final Long key;
    private final int valueSize;
    private final byte[] value;

    public DataEntry(Long key, byte[] value) {
        this.key = key;
        this.valueSize = value.length;
        this.value = value;
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
        return Objects.hash(key, valueSize, Arrays.hashCode(value));
    }

}
