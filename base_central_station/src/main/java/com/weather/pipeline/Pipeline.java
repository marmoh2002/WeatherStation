package com.weather.pipeline;

import com.weather.bitcask.engine.*;
import com.weather.archive.engine.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.weather.model.StatusMessage;

public class Pipeline {
    private final BitCaskStore store;
    private final ParquetArchiver archiver;
    private final Logger logger = LogManager.getLogger(Pipeline.class);

    public Pipeline(BitCaskStore store, ParquetArchiver archiver) {
        this.store = store;
        this.archiver = archiver;
    }

    public void process(StatusMessage record) {
        // 1. store latest reading
        try {
            store.put(record.getStationId(), record);
        } catch (Exception e) {
            logger.error("Failed to store record for station {}: {}", record.getStationId(), e.getMessage());
        }
        // 2. archive all readings
        try {
            archiver.add(record);
        } catch (Exception e) {
            logger.error("Failed to archive record for station {}: {}", record.getStationId(), e.getMessage());
        }
    }
}