package com.weather.archive.model;

import java.util.Objects;

import com.weather.archive.model.TimeBucketExtractor.TimeBucket;
import com.weather.model.StatusMessage;

public class PartitionKey {
    private final Long stationId;
    private final TimeBucket timeBucket;

    public PartitionKey(StatusMessage message) {
        this.stationId = message.getStationId();
        this.timeBucket = TimeBucketExtractor.getTimeBucketFromSeconds(message.getStatusTimestamp());
    }

    public String getPathString() {
        return String.format("station_id=%d/year=%d/month=%02d/day=%02d/hour=%02d",
                stationId, timeBucket.year, timeBucket.month, timeBucket.day, timeBucket.hour);
    }

    public Long getStationId() {
        return stationId;
    }

    // For stale partition detection, compare just the current hour bucket
    public String getCurrentHourBucket() {
        return String.format("%d-%02d-%02d-%02d",
                timeBucket.year, timeBucket.month, timeBucket.day, timeBucket.hour);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null || getClass() != other.getClass())
            return false;
        PartitionKey that = (PartitionKey) other;
        return Objects.equals(stationId, that.stationId) && Objects.equals(timeBucket.year, that.timeBucket.year)
                && Objects.equals(timeBucket.month, that.timeBucket.month)
                && Objects.equals(timeBucket.day, that.timeBucket.day)
                && Objects.equals(timeBucket.hour, that.timeBucket.hour);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stationId, timeBucket.year, timeBucket.month, timeBucket.day, timeBucket.hour);
    }
}