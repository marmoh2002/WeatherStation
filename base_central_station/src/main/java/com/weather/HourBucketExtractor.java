package com.weather;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class HourBucketExtractor {

    // 1. Define your formatter.
    // DateTimeFormatter is thread-safe, so it's best practice to make it a static
    // constant.
    private static final DateTimeFormatter HOUR_BUCKET_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
            .withZone(ZoneId.of("UTC")); // Change "UTC" to ZoneId.systemDefault() if needed

    public static String getHourBucketFromSeconds(long epochSeconds) {
        Instant instant = Instant.ofEpochSecond(epochSeconds);
        return HOUR_BUCKET_FORMATTER.format(instant);
        // output will look like "2024-06-01-14" for June 1, 2024 at 2pm UTC
    }
}