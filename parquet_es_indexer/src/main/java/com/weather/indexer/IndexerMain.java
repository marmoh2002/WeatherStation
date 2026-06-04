package com.weather.indexer;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.weather.model.StatusMessage;

public class IndexerMain {

    private static final Logger logger = LogManager.getLogger(IndexerMain.class);

    public static void main(String[] args) throws Exception {

        IndexerConfig config = new IndexerConfig();

        logger.info("Starting parquet-es-indexer");
        logger.info("  ES URL:       {}", config.elasticsearchUrl);
        logger.info("  Index:        {}", config.indexName);
        logger.info("  Parquet root: {}", config.parquetBasePath);
        logger.info("  Poll every:   {}s", config.pollIntervalSec);
        logger.info("  State file:   {}", config.stateFilePath);

        // --- one-time setup ---
        ElasticsearchBulkIndexer esIndexer = new ElasticsearchBulkIndexer(config.elasticsearchUrl, config.indexName);

        esIndexer.waitForCluster(); // blocks until ES is reachable
        esIndexer.ensureIndex(); // creates index + mappings if missing

        IndexedFileTracker tracker = new IndexedFileTracker(config.stateFilePath);
        ParquetFileScanner scanner = new ParquetFileScanner(config.parquetBasePath, tracker);
        ParquetRecordReader reader = new ParquetRecordReader();
        StatusDocumentMapper mapper = new StatusDocumentMapper();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(
                () -> runIndexingCycle(scanner, reader, mapper, esIndexer, tracker),
                0,
                config.pollIntervalSec,
                TimeUnit.SECONDS);

        new CountDownLatch(1).await();
    }

    private static void runIndexingCycle(
            ParquetFileScanner scanner,
            ParquetRecordReader reader,
            StatusDocumentMapper mapper,
            ElasticsearchBulkIndexer esIndexer,
            IndexedFileTracker tracker) {
        try {
            var newFiles = scanner.findNewFiles();

            if (newFiles.isEmpty()) {
                logger.info("No new Parquet files found");
            } else {
                logger.info("Found {} new file(s) to index", newFiles.size());
            }

            for (var file : newFiles) {
                logger.info("Reading {}", file);

                List<StatusMessage> records = reader.readAll(file);
                logger.info("  {} records read", records.size());

                List<Map.Entry<String, Map<String, Object>>> docs = records.stream()
                        .map(r -> new AbstractMap.SimpleEntry<>(
                                mapper.docId(r),
                                mapper.toDocument(r)))
                        .collect(Collectors.toList());

                esIndexer.bulkIndex(docs);

                tracker.markIndexed(file);
                logger.info("  Marked as indexed: {}", file.getFileName());
            }
        } catch (Throwable e) {
            logger.error("error during indexing cycle", e);
            System.err.println(">>> Exception: " + e.getMessage());
            System.err.println(">>> Cause: " + e.getCause());
            e.printStackTrace(System.err);
            throw new RuntimeException(e);
        } finally {
            logger.info("Indexing cycle completed");
        }

    }
}