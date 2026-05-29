package com.weather;

import java.util.Objects;

import com.weather.model.StatusMessage;

public class PartitionKey {
    private final Long stationId;
    private final String hourBucket;

    public PartitionKey(StatusMessage message) {
        this.stationId = message.getStationId();
        this.hourBucket = HourBucketExtractor.getHourBucketFromSeconds(message.getStatusTimestamp());
    }

    public String getPathString() {
        return "station_id=" + stationId + "/" + hourBucket;
    }

    public Long getStationId() {
        return stationId;
    }

    public String getHourBucket() {
        return hourBucket;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null || getClass() != other.getClass())
            return false;
        PartitionKey that = (PartitionKey) other;
        return Objects.equals(this.stationId, that.stationId) && Objects.equals(this.hourBucket, that.hourBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stationId, hourBucket);
    }

}