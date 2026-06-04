package com.weather.indexer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import com.weather.model.StatusMessage;
import com.weather.model.WeatherReading;

public class ParquetRecordReader {
    private static final Logger logger = LogManager.getLogger(ParquetRecordReader.class);
    public List<StatusMessage> readAll(Path filePath) throws IOException {
        logger.info("Reading all records from {}", filePath);
        Configuration conf = new Configuration();
        conf.setBoolean("hadoop.native.lib", false);

        org.apache.hadoop.fs.Path hadoopPath =
            new org.apache.hadoop.fs.Path(filePath.toAbsolutePath().toString());

        List<StatusMessage> records = new ArrayList<>();    
        logger.info("Reading all records from {}", filePath);
        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(
                        HadoopInputFile.fromPath(hadoopPath, conf))
                    .withDataModel(GenericData.get())
                    .build()) {

            GenericRecord record;
            while ((record = reader.read()) != null) {
                StatusMessage msg = new StatusMessage();
                msg.setStationId((Long) record.get("station_id"));
                msg.setSNo((Long) record.get("s_no"));
                msg.setBatteryStatus(record.get("battery_status").toString());
                msg.setStatusTimestamp((Long) record.get("status_timestamp"));
                logger.info("Read record: {}", msg);

                GenericRecord w = (GenericRecord) record.get("weather");
                if (w != null) {
                    logger.info("Read weather: {}", w);
                    WeatherReading weather = new WeatherReading();
                    weather.setHumidity((Integer) w.get("humidity"));
                    weather.setTemperature((Integer) w.get("temperature"));
                    weather.setWindSpeed((Integer) w.get("wind_speed"));
                    msg.setWeather(weather);
                }

                records.add(msg);
            }
        } catch (Exception e) {
            logger.error("Error reading records from {}", filePath, e);
            throw e;
        }
        logger.info("Read {} records from {}", records.size(), filePath);
        return records;
    }
}