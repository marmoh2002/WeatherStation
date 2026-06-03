package com.weather.indexer;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.weather.model.StatusMessage;

public class StatusDocumentMapper {
    private static final Logger logger = LogManager.getLogger(StatusDocumentMapper.class);
    public String docId(StatusMessage msg) {
        logger.info("Mapping document ID for message: {}", msg);
        return msg.getStationId() + "_" + msg.getSNo();
    }

    public Map<String, Object> toDocument(StatusMessage msg) {
        logger.info("Mapping document for message: {}", msg);
        Map<String, Object> doc = new HashMap<>();

        doc.put("station_id", msg.getStationId());
        doc.put("s_no", msg.getSNo());
        doc.put("battery_status", msg.getBatteryStatus());
        
        doc.put("status_timestamp", msg.getStatusTimestamp());

        // ISO-8601 string — Kibana needs this for the time picker
        doc.put("@timestamp",
                Instant.ofEpochSecond(msg.getStatusTimestamp()).toString());

        // Flatten weather fields
        if (msg.getWeather() != null) {
            doc.put("humidity", msg.getWeather().getHumidity());
            doc.put("temperature", msg.getWeather().getTemperature());
            doc.put("wind_speed", msg.getWeather().getWindSpeed());
        }
        logger.info("Mapped document: {}", doc);
        return doc;
    }
}