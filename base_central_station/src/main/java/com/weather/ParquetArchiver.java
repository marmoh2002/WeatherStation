package com.weather;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.nio.file.Paths;
import java.time.Instant;
import java.nio.file.Files;
import java.io.IOException;
import com.weather.config.AppConfigs;
import com.weather.model.StatusMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ParquetArchiver {

    private static final Logger logger = LogManager.getLogger(ParquetArchiver.class);
    private final int BATCH_SIZE = AppConfigs.getParquetBatchSize();
    private final String baseOutputPath;
    private final ConcurrentHashMap<PartitionKey, List<StatusMessage>> buffers;
    private final ScheduledExecutorService scheduler;
    private final Configuration hadoopConf;
    private static final AtomicInteger fileSequence = new AtomicInteger(0);
    private volatile boolean closed = false;

    public ParquetArchiver(String baseOutputPath) {
        this.baseOutputPath = (baseOutputPath != null && !baseOutputPath.isBlank())
                ? baseOutputPath
                : AppConfigs.getParquetOutputBasePath();
        this.buffers = new ConcurrentHashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.hadoopConf = new Configuration();

        logger.info("Starting ParquetArchiver background thread for flushing stale partitions...");
        scheduler.scheduleAtFixedRate(this::flushStalePartitions, 0, 30, TimeUnit.MINUTES);
    }

    // Called by poll loop — only public entry point
    public void add(StatusMessage record) {
        if (closed) {
            logger.warn("Archiver is closed, dropping record.");
            return;
        }
        PartitionKey key = new PartitionKey(record);
        AtomicReference<List<StatusMessage>> batchToFlushRef = new AtomicReference<>();
        buffers.compute(key, (k, batch) -> {
            if (batch == null) {
                batch = new ArrayList<>();
            }
            batch.add(record);
            if (batch.size() >= BATCH_SIZE) {
                batchToFlushRef.set(batch);
                return null; // removes the batch with key k from the map
            }
            return batch;
        });
        // Flush happens OUTSIDE the map lock so we don't block other threads
        if (batchToFlushRef.get() != null) {
            flush(key, batchToFlushRef.get());
        }
    }

    // Called by background thread — scans for stale hour buckets
    private void flushStalePartitions() {
        // iterate over buffers
        // extract each key
        try {
            String currentHourBucket = HourBucketExtractor.getHourBucketFromSeconds(Instant.now().getEpochSecond());
            buffers.forEach((key, batch) -> {
                if (!key.getHourBucket().equals(currentHourBucket)) {
                    AtomicReference<List<StatusMessage>> staleBatchRef = new AtomicReference<>();
                    buffers.computeIfPresent(key, (k, currBatch) -> {
                        staleBatchRef.set(currBatch);
                        return null; // remove stale batch from map
                    });
                    // Flush happens OUTSIDE the map lock so we don't block other threads
                    if (staleBatchRef.get() != null && !staleBatchRef.get().isEmpty()) {
                        flush(key, staleBatchRef.get());
                    }
                }

            });
        } catch (Throwable e) {
            logger.error("Error while flushing stale partitions: ", e);
        }
    }

    // Flushes one specific partition to disk
    private void flush(PartitionKey key, List<StatusMessage> batch) {
        // 1. Convert batch to Parquet format (using Avro schema)
        // 2. Write to path: baseOutputPath/key.getPathString()/timestamp.parquet
        // (timestamp can be current time or max timestamp in batch)
        try {
            java.nio.file.Path localDirPath = Paths.get(baseOutputPath, key.getPathString());
            Files.createDirectories(localDirPath);
            int seq = fileSequence.incrementAndGet() % 1000;
            String fileName = "data_" + key.getStationId() + "_" + key.getHourBucket() + "_" + seq + ".parquet";
            Path parquetFilePath = new Path(localDirPath.toString(), fileName);
            // try-with-resources statement. No need to explicitly close the writer after
            // curly braces execute.
            try (ParquetWriter<StatusMessage> writer = AvroParquetWriter.<StatusMessage>builder(
                    HadoopOutputFile.fromPath(parquetFilePath, hadoopConf))
                    .withSchema(StatusMessage.getClassSchema())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build()) {
                for (StatusMessage record : batch) {
                    writer.write(record);
                }
                logger.debug("Successfully flushed {} records to {}", batch.size(), parquetFilePath);
            }
        } catch (IOException e) { // catch all to prevent background thread from dying. We log the error and move
                                  // on, but in a real system we might want to implement retries or alerting here.
            logger.error("Error while flushing partition " + key.getPathString() + ": ", e);
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
