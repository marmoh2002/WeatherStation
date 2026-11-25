package com.weather.pipeline;

import com.weather.bitcask.engine.*;
import com.weather.archive.engine.*;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.weather.model.StatusMessage;

public class Pipeline {
    private final BitCaskStore store;
    private final ParquetArchiver archiver;
    private final Logger logger = LogManager.getLogger(Pipeline.class);
    ConcurrentHashMap<Long, Long> lastSeenSNo = new ConcurrentHashMap<>();

    public Pipeline(BitCaskStore store, ParquetArchiver archiver) {
        this.store = store;
        this.archiver = archiver;
    }

    public void process(StatusMessage record) {
        long stationId = record.getStationId();
        long currentSNo = record.getSNo();

        // 1. Check for missing sequence numbers
        Long previousSNo = lastSeenSNo.get(stationId);
        if (previousSNo != null && currentSNo - previousSNo > 1) {
            long droppedCount = (currentSNo - previousSNo) - 1;
            logger.warn("Station {} dropped {} packet(s) between s_no {} and {}",
                    stationId, droppedCount, previousSNo, currentSNo);
            // Synthesize drop marker records for historical Parquet archival
            for (long missingSNo = previousSNo + 1; missingSNo < currentSNo; missingSNo++) {
                StatusMessage dropRecord = StatusMessage.newBuilder()
                        .setStationId(stationId)
                        .setSNo(missingSNo)
                        .setStatus("DROPPED")
                        .setBatteryStatus("unknown")
                        .setStatusTimestamp(record.getStatusTimestamp())
                        .setWeather(null) // allowed now by our updated Avro schema!
                        .build();
                try {
                    archiver.add(dropRecord);
                } catch (Exception e) {
                    logger.error("Failed to archive drop record for station {}: {}", stationId, e.getMessage());
                }
            }
        }
        lastSeenSNo.put(stationId, currentSNo);

        // 2. store latest reading
        try {
            store.put(record.getStationId(), record);
        } catch (Exception e) {
            logger.error("Failed to store record for station {}: {}", record.getStationId(), e.getMessage());
        }

        // 3. archive all readings
        try {
            archiver.add(record);
        } catch (Exception e) {
            logger.error("Failed to archive record for station {}: {}", record.getStationId(), e.getMessage());
        }
    }
}