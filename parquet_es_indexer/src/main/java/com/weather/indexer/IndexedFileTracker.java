package com.weather.indexer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
public class IndexedFileTracker {

    private static final Logger logger = LogManager.getLogger(IndexedFileTracker.class);
    private final Path stateFile;
    private final Set<String> indexedPaths = new HashSet<>();

    public IndexedFileTracker(String stateFilePath) throws IOException {
        logger.info("Creating IndexedFileTracker at path: {}", stateFilePath);
        this.stateFile = Paths.get(stateFilePath);
        // Create parent dirs if they don't exist
        Files.createDirectories(stateFile.getParent());
        // Load existing state
        if (Files.exists(stateFile)) {
            logger.info("Loading existing indexed paths from: {}", stateFile);
            Files.lines(stateFile)
                 .map(String::trim)
                 .filter(l -> !l.isEmpty())
                 .forEach(indexedPaths::add);
        }
        logger.info("IndexedFileTracker created with {} paths", indexedPaths.size());
    }

    public boolean isIndexed(Path filePath) {
        logger.info("Checking if {} is indexed", filePath);
        return indexedPaths.contains(filePath.toAbsolutePath().toString());
    }

    public void markIndexed(Path filePath) throws IOException {
        String abs = filePath.toAbsolutePath().toString();
        logger.info("Marking {} as indexed", abs);
        indexedPaths.add(abs);
        // Append so we never rewrite the whole file
        try (BufferedWriter w = Files.newBufferedWriter(
                stateFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            logger.info("Writing to state file: {}", abs);
            w.write(abs);
            w.newLine();
        }
        logger.info("Marked {} as indexed", abs);
    }
}