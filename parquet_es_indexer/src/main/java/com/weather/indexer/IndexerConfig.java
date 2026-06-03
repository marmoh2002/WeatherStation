package com.weather.indexer;

public class IndexerConfig {
    public final String elasticsearchUrl;
    public final String indexName;
    public final String parquetBasePath;
    public final int pollIntervalSec;
    public final String stateFilePath;

    public IndexerConfig() {
        this.elasticsearchUrl  = getEnv("ELASTICSEARCH_URL",       "http://localhost:9200");
        this.indexName         = getEnv("ELASTICSEARCH_INDEX",     "weather-status");
        this.parquetBasePath   = getEnv("PARQUET_OUTPUT_BASE_PATH", "/app/parquet_output");
        this.pollIntervalSec   = Integer.parseInt(getEnv("INDEXER_POLL_INTERVAL_SEC", "30"));
        this.stateFilePath     = getEnv("INDEXER_STATE_FILE",      "/app/state/indexed-files.txt");
    }

    private String getEnv(String key, String fallback) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : fallback;
    }
}
