package com.weather.indexer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
public class ParquetFileScanner {
    private static final Logger logger = LogManager.getLogger(ParquetFileScanner.class);
    private final Path baseDir;
    private final IndexedFileTracker tracker;

    public ParquetFileScanner(String basePath, IndexedFileTracker tracker) {
        logger.info("Creating ParquetFileScanner at path: {}", basePath);
        this.baseDir = Paths.get(basePath);
        this.tracker = tracker;
    }

    public List<Path> findNewFiles() throws IOException {
        logger.info("Finding new files in {}", baseDir);
        if (!Files.exists(baseDir)) return List.of();

        try (var stream = Files.walk(baseDir)) {
            logger.info("Walking files in {}", baseDir);
            return stream
                .filter(p -> p.toString().endsWith(".parquet"))
                .filter(Files::isRegularFile)
                .filter(p -> !tracker.isIndexed(p))
                .peek(p -> logger.info("Found new file: {}", p))
                .sorted()   // process oldest first
                .collect(Collectors.toList());
        }
    }
}