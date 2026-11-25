package com.weather.pipeline;

import com.weather.bitcask.engine.*;
import com.weather.archive.engine.*;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.weather.model.StatusMessage;

public class Pipeline {
    public static final int PARQUET_QUEUE_CAPACITY = 10_000;
    private static final int HIGH_WATERMARK = (int) (PARQUET_QUEUE_CAPACITY * 0.80);
    private static final int LOW_WATERMARK = (int) (PARQUET_QUEUE_CAPACITY * 0.50);

    private final BitCaskStore store;
    private final ParquetArchiver archiver;
    private final Logger logger = LogManager.getLogger(Pipeline.class);
    private final ConcurrentHashMap<Long, Long> lastSeenSNo = new ConcurrentHashMap<>();
    private final BlockingQueue<StatusMessage> parquetQueue = new ArrayBlockingQueue<>(PARQUET_QUEUE_CAPACITY);
    private final ExecutorService parquetWorker = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "parquet-async-writer");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean running = true;

    public Pipeline(BitCaskStore store, ParquetArchiver archiver) {
        this.store = store;
        this.archiver = archiver;
        startParquetWorker();
    }

    private void startParquetWorker() {
        parquetWorker.submit(() -> {
            while (running || !parquetQueue.isEmpty()) {
                try {
                    StatusMessage msg = parquetQueue.poll(500, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        archiver.add(msg);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Async Parquet worker failed to archive record: {}", e.getMessage(), e);
                }
            }
        });
    }

    public boolean isParquetQueueHigh() {
        return parquetQueue.size() >= HIGH_WATERMARK;
    }

    public boolean isParquetQueueLow() {
        return parquetQueue.size() <= LOW_WATERMARK;
    }

    public int getParquetQueueSize() {
        return parquetQueue.size();
    }

    public void shutdown() {
        running = false;
        parquetWorker.shutdown();
        try {
            if (!parquetWorker.awaitTermination(15, TimeUnit.SECONDS)) {
                parquetWorker.shutdownNow();
            }
        } catch (InterruptedException e) {
            parquetWorker.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void enqueueParquet(StatusMessage msg) {
        try {
            if (!parquetQueue.offer(msg)) {
                logger.warn("Parquet queue full ({} items); blocking until space frees.", PARQUET_QUEUE_CAPACITY);
                parquetQueue.put(msg);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while enqueuing record for Parquet archival.");
        }
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
                enqueueParquet(dropRecord);
            }
        }
        lastSeenSNo.put(stationId, currentSNo);

        // 2. store latest reading
        try {
            store.put(record.getStationId(), record);
        } catch (Exception e) {
            logger.error("Failed to store record for station {}: {}", record.getStationId(), e.getMessage());
        }

        // 3. enqueue for asynchronous Parquet archival
        enqueueParquet(record);
    }
}