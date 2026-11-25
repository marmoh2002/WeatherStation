package com.weather.indexer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IndexedFileTracker {

    private static final Logger logger = LogManager.getLogger(IndexedFileTracker.class);
    private final Path stateFile;
    private final Map<String, LedgerEntry> indexedEntries = new HashMap<>();

    public record LedgerEntry(long modifiedTimeMs, String sha256) {
    }

    public IndexedFileTracker(String stateFilePath) throws IOException {
        logger.info("Creating IndexedFileTracker at path: {}", stateFilePath);
        this.stateFile = Paths.get(stateFilePath);
        Files.createDirectories(stateFile.getParent());
        if (Files.exists(stateFile)) {
            logger.info("Loading existing indexed ledger from: {}", stateFile);
            try (var lines = Files.lines(stateFile)) {
                lines.map(String::trim)
                        .filter(l -> !l.isEmpty())
                        .forEach(this::parseLedgerLine);
            }
        }
        logger.info("IndexedFileTracker created with {} ledger entries", indexedEntries.size());
    }

    private void parseLedgerLine(String line) {
        String[] parts = line.split("\t", 3);
        if (parts.length == 3) {
            try {
                long mtime = Long.parseLong(parts[1]);
                indexedEntries.put(parts[0], new LedgerEntry(mtime, parts[2]));
            } catch (NumberFormatException e) {
                logger.warn("Skipping malformed ledger line: {}", line);
            }
        } else {
            // Backward compatibility for legacy path-only entries
            indexedEntries.put(parts[0], new LedgerEntry(-1L, ""));
        }
    }

    public boolean isIndexed(Path filePath) {
        String abs = filePath.toAbsolutePath().toString();
        LedgerEntry entry = indexedEntries.get(abs);
        if (entry == null) {
            return false;
        }
        if (entry.modifiedTimeMs() < 0) {
            return true; // legacy path-only entry
        }
        try {
            long currentMtime = Files.getLastModifiedTime(filePath).toMillis();
            if (currentMtime == entry.modifiedTimeMs()) {
                return true;
            }
            // Modification time changed; verify content via SHA-256
            String currentHash = computeSha256(filePath);
            return currentHash.equals(entry.sha256());
        } catch (IOException e) {
            logger.warn("Failed to stat/hash file {} during ledger check: {}", abs, e.getMessage());
            return false;
        }
    }

    public void markIndexed(Path filePath) throws IOException {
        String abs = filePath.toAbsolutePath().toString();
        long mtime = Files.getLastModifiedTime(filePath).toMillis();
        String sha256 = computeSha256(filePath);

        indexedEntries.put(abs, new LedgerEntry(mtime, sha256));
        String line = abs + "\t" + mtime + "\t" + sha256;
        try (BufferedWriter w = Files.newBufferedWriter(
                stateFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            w.write(line);
            w.newLine();
        }
        logger.info("Marked {} as indexed (mtime={}, sha256={})", abs, mtime, sha256);
    }

    private static String computeSha256(Path filePath) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            try (InputStream in = Files.newInputStream(filePath)) {
                int read;
                while ((read = in.read(buffer)) != -1) {
                    digest.update(buffer, 0, read);
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm unavailable", e);
        }
    }
}