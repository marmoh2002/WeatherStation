package com.weather.indexer;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class ElasticsearchBulkIndexer {

    private static final Logger logger = LogManager.getLogger(ElasticsearchBulkIndexer.class);
    private static final int BATCH_SIZE = 500;
    private static final int CLUSTER_RETRY_DELAY_SECONDS = 5;

    private final ElasticsearchClient client;
    private final String indexName;

    public ElasticsearchBulkIndexer(String esUrl, String indexName) {
        URI uri = URI.create(esUrl);
        RestClient restClient = RestClient.builder(
            new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme())
        ).build();
        this.client    = new ElasticsearchClient(
            new RestClientTransport(restClient, new JacksonJsonpMapper()));
        this.indexName = indexName;
    }

    /** Block until ES responds (called once at startup). */
    public void waitForCluster() throws InterruptedException {
        logger.info("Waiting for Elasticsearch at index={}", indexName);
        while (!isClusterReachable()) {
            logger.warn("ES not ready yet, retrying in {}s...", CLUSTER_RETRY_DELAY_SECONDS);
            awaitRetryDelay();
        }
        logger.info("Elasticsearch is up");
    }

    private boolean isClusterReachable() {
        try {
            client.ping();
            return true;
        } catch (IOException | ElasticsearchException e) {
            return false;
        }
    }

    private static void awaitRetryDelay() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(CLUSTER_RETRY_DELAY_SECONDS));
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
    }

    /** Create index with basic mappings if it doesn't exist yet. */
    public void ensureIndex() throws IOException {
        boolean exists = client.indices().exists(r -> r.index(indexName)).value();
        if (exists) {
            logger.info("Index '{}' already exists", indexName);
            return;
        }
        client.indices().create(CreateIndexRequest.of(r -> r
            .index(indexName)
            .mappings(m -> m
                .properties("station_id",       p -> p.long_(f -> f))
                .properties("s_no",             p -> p.long_(f -> f))
                .properties("battery_status",   p -> p.keyword(f -> f))
                .properties("status_timestamp", p -> p.long_(f -> f))
                .properties("@timestamp",       p -> p.date(f -> f))
                .properties("humidity",         p -> p.integer(f -> f))
                .properties("temperature",      p -> p.integer(f -> f))
                .properties("wind_speed",       p -> p.integer(f -> f))
            )
        ));
        logger.info("Created index '{}'", indexName);
    }

    /** Index a list of (id, document) pairs in batches of BATCH_SIZE. */
    public void bulkIndex(List<Map.Entry<String, Map<String, Object>>> docs) throws IOException {
        for (int i = 0; i < docs.size(); i += BATCH_SIZE) {
            List<Map.Entry<String, Map<String, Object>>> batch =
                docs.subList(i, Math.min(i + BATCH_SIZE, docs.size()));

            BulkRequest.Builder br = new BulkRequest.Builder();
            for (var entry : batch) {
                String id  = entry.getKey();
                var    doc = entry.getValue();
                br.operations(op -> op.index(idx -> idx
                    .index(indexName)
                    .id(id)
                    .document(doc)
                ));
            }

            BulkResponse response = client.bulk(br.build());
            if (response.errors()) {
                response.items().forEach(item -> {
                    var error = item.error();
                    if (error != null) {
                        logger.error("Bulk error id={} reason={}", item.id(), error.reason());
                    }
                });
            } else {
                logger.info("Indexed batch of {} docs", batch.size());
            }
        }
    }
}