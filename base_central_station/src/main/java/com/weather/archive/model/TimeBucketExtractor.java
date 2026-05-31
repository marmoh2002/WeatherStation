package com.weather.archive.model;

import java.time.Instant;
import java.time.ZoneId;
import java.time.LocalDateTime;
import java.util.Objects;

public class TimeBucketExtractor {
    private static final ZoneId ZONE = ZoneId.of("UTC");

    public static class TimeBucket {
        public final int year;
        public final int month;
        public final int day;
        public final int hour;

        public TimeBucket(int year, int month, int day, int hour) {
            this.year = year;
            this.month = month;
            this.day = day;
            this.hour = hour;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other == null || getClass() != other.getClass())
                return false;
            TimeBucket that = (TimeBucket) other;
            return this.year == that.year && this.month == that.month && this.day == that.day
                    && this.hour == that.hour;
        }

        @Override
        public int hashCode() {
            return Objects.hash(year, month, day, hour);
        }

        @Override
        public String toString() {
            return String.format("%d-%02d-%02d-%02d", year, month, day, hour);
        }
    }

    public static TimeBucket getTimeBucketFromSeconds(long epochSeconds) {
        Instant instant = Instant.ofEpochSecond(epochSeconds);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZONE);
        return new TimeBucket(
                localDateTime.getYear(),
                localDateTime.getMonthValue(),
                localDateTime.getDayOfMonth(),
                localDateTime.getHour());
    }
}