package com.weather.archive.engine;

import java.util.UUID;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import com.weather.archive.model.PartitionKey;
import com.weather.archive.model.TimeBucketExtractor;
import com.weather.config.AppConfigs;
import com.weather.model.StatusMessage;

public class ParquetArchiver {

    private static final Logger logger = LogManager.getLogger(ParquetArchiver.class);
    private final int BATCH_SIZE = AppConfigs.getParquetBatchSize();
    private final int FLUSH_INTERVAL_SECS = AppConfigs.getParquetFlushIntervalSecs();
    private final String baseOutputPath;
    private final ConcurrentHashMap<PartitionKey, List<StatusMessage>> buffers;
    private final ConcurrentHashMap<PartitionKey, Long> batchStartTimes;
    private final ScheduledExecutorService scheduler;
    private final Configuration hadoopConf;
    private volatile boolean closed = false;

    public ParquetArchiver(String baseOutputPath) {
        this.baseOutputPath = (baseOutputPath != null && !baseOutputPath.isBlank())
                ? baseOutputPath
                : AppConfigs.getParquetOutputBasePath();
        this.buffers = new ConcurrentHashMap<>();
        this.batchStartTimes = new ConcurrentHashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.hadoopConf = new Configuration();

        logger.info("Starting ParquetArchiver background thread (dual-trigger flush every {}s)...",
                FLUSH_INTERVAL_SECS);
        scheduler.scheduleAtFixedRate(this::flushStalePartitions, FLUSH_INTERVAL_SECS, FLUSH_INTERVAL_SECS,
                TimeUnit.SECONDS);

    }

    // Called by poll loop — only public entry point
    public void add(StatusMessage record) {
        if (closed) {
            logger.warn("Archiver is closed, dropping record.");
            return;
        }

        PartitionKey key = new PartitionKey(record);
        logger.info("Processing record for partition: {}", key.getPathString());
        AtomicReference<List<StatusMessage>> batchToFlushRef = new AtomicReference<>();
        buffers.compute(key, (k, batch) -> {
            if (batch == null) {
                batch = new ArrayList<>();
                batchStartTimes.putIfAbsent(k, System.currentTimeMillis());
            }
            batch.add(record);
            logger.debug("Batch size now: {}/{}", batch.size(), BATCH_SIZE);

            if (batch.size() >= BATCH_SIZE) {
                logger.info("Batch full! Flushing partition: {}", key.getPathString());
                batchToFlushRef.set(batch);
                batchStartTimes.remove(k);
                return null; // removes the batch with key k from the map
            }
            return batch;
        });
        // Flush happens OUTSIDE the map lock so we don't block other threads
        if (batchToFlushRef.get() != null) {
            flush(key, batchToFlushRef.get());
        }
    }

    // Called by background thread — flushes stale hour buckets and batches older than FLUSH_INTERVAL_SECS
    private void flushStalePartitions() {
        try {
            logger.debug("STALE_FLUSH: Checking for stale or timed-out partitions...");
            String currentHourBucket = TimeBucketExtractor.getTimeBucketFromSeconds(Instant.now().getEpochSecond())
                    .toString();
            long now = System.currentTimeMillis();
            long maxAgeMs = FLUSH_INTERVAL_SECS * 1000L;

            buffers.forEach((key, batch) -> {
                Long startTime = batchStartTimes.get(key);
                boolean isPreviousHour = !key.getCurrentHourBucket().equals(currentHourBucket);
                boolean isTimedOut = startTime != null && (now - startTime) >= maxAgeMs;

                if (isPreviousHour || isTimedOut) {
                    AtomicReference<List<StatusMessage>> staleBatchRef = new AtomicReference<>();
                    buffers.computeIfPresent(key, (k, currBatch) -> {
                        staleBatchRef.set(currBatch);
                        batchStartTimes.remove(k);
                        return null; // remove stale batch from map
                    });
                    // Flush happens OUTSIDE the map lock so we don't block other threads
                    if (staleBatchRef.get() != null && !staleBatchRef.get().isEmpty()) {
                        logger.info("STALE_FLUSH: Flushing {} records from partition {} (previousHour={}, timedOut={})",
                                staleBatchRef.get().size(), key.getPathString(), isPreviousHour, isTimedOut);
                        flush(key, staleBatchRef.get());
                    }
                }

            });
        } catch (Throwable e) {
            logger.error("Error while flushing stale partitions: ", e);
        }
    }

    // Flushes one specific partition to disk atomically via .tmp -> .parquet
    private void flush(PartitionKey key, List<StatusMessage> batch) {
        java.nio.file.Path tmpLocalPath = null;
        try {
            java.nio.file.Path localDirPath = Paths.get(baseOutputPath, key.getPathString());
            logger.info("Creating directories: {}", localDirPath);
            Files.createDirectories(localDirPath);
            String fileName = "weather_data_" + UUID.randomUUID() + ".parquet";
            tmpLocalPath = localDirPath.resolve(fileName + ".tmp");
            java.nio.file.Path finalLocalPath = localDirPath.resolve(fileName);
            Path tmpParquetPath = new Path(tmpLocalPath.toString());
            logger.info("Writing temporary parquet file: {}", tmpParquetPath);
            try (ParquetWriter<StatusMessage> writer = AvroParquetWriter.<StatusMessage>builder(
                    HadoopOutputFile.fromPath(tmpParquetPath, hadoopConf))
                    .withSchema(StatusMessage.getClassSchema())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build()) {
                for (StatusMessage record : batch) {
                    writer.write(record);
                }
            }
            Files.move(tmpLocalPath, finalLocalPath,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            logger.info("Successfully flushed and committed {} records to {}", batch.size(), finalLocalPath);
        } catch (IOException e) {
            logger.error("Error while flushing partition " + key.getPathString() + ": ", e);
            if (tmpLocalPath != null) {
                try {
                    Files.deleteIfExists(tmpLocalPath);
                } catch (IOException cleanupEx) {
                    logger.warn("Failed to clean up temp file {}: {}", tmpLocalPath, cleanupEx.getMessage());
                }
            }
        }
    }

    // Called on shutdown
    public void close() {
        closed = true;
        logger.info("Shutting down ParquetArchiver, flushing all remaining batches...");
        scheduler.shutdown(); // stop the background thread
        try {
            // Wait up to 5 seconds for the current sweeping task to finish
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        buffers.forEach((key, batch) -> {
            if (batch != null && !batch.isEmpty()) {
                flush(key, batch);
            }
        });
        buffers.clear(); // clear buffers to free memory after flushing
        logger.info("ParquetArchiver shutdown complete.");
    }

}
