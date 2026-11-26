package com.weather.bitcask.server;

import com.weather.bitcask.engine.BitCaskStore;
import com.weather.bitcask.engine.Compactor;
import com.weather.model.StatusMessage;
import com.weather.model.WeatherReading;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BitCaskServerTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private BitCaskStore store;
    private BitCaskServer server;
    private HttpClient httpClient;
    private int port;

    private static StatusMessage makeMessage(long stationId, long sNo) {
        return StatusMessage.newBuilder()
                .setStationId(stationId)
                .setSNo(sNo)
                .setBatteryStatus("ok")
                .setStatusTimestamp(1_700_000_000L + sNo)
                .setWeather(WeatherReading.newBuilder()
                        .setHumidity(55)
                        .setTemperature(21)
                        .setWindSpeed(8)
                        .build())
                .build();
    }

    @Before
    public void setUp() throws Exception {
        Path dir = tempDir.newFolder("server-store").toPath();
        store = new BitCaskStore(dir.toString(), 96L);
        server = new BitCaskServer(store, 0);
        server.start();
        port = server.getPort();
        httpClient = HttpClient.newHttpClient();
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop();
        }
        if (store != null) {
            store.close();
        }
    }

    private HttpResponse<String> get(String pathAndQuery) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + pathAndQuery))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    @Test
    public void testViewKeyStatusCodesAndQueryParsing() throws Exception {
        // Insert enough keys to force segment rotation
        store.put(101L, makeMessage(101L, 1L));
        store.put(102L, makeMessage(102L, 2L));
        store.put(103L, makeMessage(103L, 3L));
        store.put(104L, makeMessage(104L, 4L));
        store.put(105L, makeMessage(105L, 5L));

        // 200 for sealed segment key and active segment key
        HttpResponse<String> okSealed = get("/view-key?id=101");
        assertEquals(200, okSealed.statusCode());
        assertTrue(okSealed.body().contains("\"station_id\": 101"));
        assertUtf8ContentType(okSealed);

        HttpResponse<String> okActive = get("/view-key?id=105");
        assertEquals(200, okActive.statusCode());
        assertTrue(okActive.body().contains("\"station_id\": 105"));
        assertUtf8ContentType(okActive);

        // 200 when extra query parameters appear before or after id
        HttpResponse<String> extraBefore = get("/view-key?foo=1&id=101");
        assertEquals(200, extraBefore.statusCode());
        assertTrue(extraBefore.body().contains("\"station_id\": 101"));
        assertUtf8ContentType(extraBefore);

        HttpResponse<String> extraAfter = get("/view-key?id=101&foo=bar");
        assertEquals(200, extraAfter.statusCode());
        assertTrue(extraAfter.body().contains("\"station_id\": 101"));
        assertUtf8ContentType(extraAfter);

        // 200 continues working after compaction
        new Compactor(store).compact();
        HttpResponse<String> afterCompact = get("/view-key?id=101");
        assertEquals(200, afterCompact.statusCode());
        assertTrue(afterCompact.body().contains("\"station_id\": 101"));
        assertUtf8ContentType(afterCompact);

        // 404 for unknown key
        HttpResponse<String> notFound = get("/view-key?id=99999");
        assertEquals(404, notFound.statusCode());
        assertEquals("Key Not Found", notFound.body());
        assertUtf8ContentType(notFound);

        // 400 for missing and malformed id query strings
        String[] badQueries = {
                "/view-key",
                "/view-key?",
                "/view-key?foo=1",
                "/view-key?foo=1&id=",
                "/view-key?id=",
                "/view-key?id",
                "/view-key?id=%20",
                "/view-key?id=+",
                "/view-key?id=%2B",
                "/view-key?id=-",
                "/view-key?id=abc",
                "/view-key?id=101=2",
                "/view-key?id=1.5",
                "/view-key?id=99999999999999999999999"
        };
        for (String path : badQueries) {
            HttpResponse<String> bad = get(path);
            assertEquals("Expected 400 for " + path, 400, bad.statusCode());
            assertUtf8ContentType(bad);
        }
    }

    @Test
    public void testMethodNotAllowedAndViewAllHeaders() throws Exception {
        store.put(201L, makeMessage(201L, 7L));

        HttpRequest postRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/view-key?id=201"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> methodNotAllowed = httpClient.send(
                postRequest, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertEquals(405, methodNotAllowed.statusCode());
        assertUtf8ContentType(methodNotAllowed);

        HttpResponse<String> viewAll = get("/view-all");
        assertEquals(200, viewAll.statusCode());
        assertTrue(viewAll.body().contains("201,"));
        assertUtf8ContentType(viewAll);
    }

    @Test
    public void testBoundedExecutorAndConcurrentViewKeyRequests() throws Exception {
        assertTrue("Expected bounded ThreadPoolExecutor", server.getExecutor() instanceof ThreadPoolExecutor);
        ThreadPoolExecutor pool = (ThreadPoolExecutor) server.getExecutor();
        assertTrue("Expected bounded maxPoolSize", pool.getMaximumPoolSize() < Integer.MAX_VALUE);
        assertTrue("Expected corePoolSize <= maxPoolSize", pool.getCorePoolSize() <= pool.getMaximumPoolSize());
        BlockingQueue<Runnable> queue = pool.getQueue();
        int totalCapacity = queue.size() + queue.remainingCapacity();
        assertTrue("Expected bounded work queue", totalCapacity < Integer.MAX_VALUE);
        assertTrue("Expected named worker threads",
                pool.getThreadFactory().newThread(() -> { }).getName().startsWith("bitcask-http-worker-"));

        int totalKeys = 12;
        for (long key = 1; key <= totalKeys; key++) {
            store.put(key, makeMessage(key, key * 10L));
        }

        int clientThreads = 16;
        int requestsPerThread = 4;
        ExecutorService clients = Executors.newFixedThreadPool(clientThreads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate = new CountDownLatch(clientThreads);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < clientThreads; t++) {
            final int threadIdx = t;
            clients.submit(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < requestsPerThread; i++) {
                        long key = ((threadIdx + i) % totalKeys) + 1L;
                        HttpResponse<String> resp = get("/view-key?id=" + key);
                        assertEquals(200, resp.statusCode());
                        assertTrue("Unexpected body for key " + key + ": " + resp.body(),
                                resp.body().contains("\"station_id\": " + key));
                    }
                } catch (Throwable ex) {
                    errors.add(ex);
                } finally {
                    doneGate.countDown();
                }
            });
        }

        startGate.countDown();
        assertTrue("Concurrent HTTP requests timed out", doneGate.await(30, TimeUnit.SECONDS));
        clients.shutdown();
        assertTrue("Client pool failed to terminate", clients.awaitTermination(5, TimeUnit.SECONDS));
        assertTrue("Unexpected errors in concurrent /view-key test: " + errors.peek(), errors.isEmpty());
    }

    private static void assertUtf8ContentType(HttpResponse<?> response) {
        Optional<String> contentType = response.headers().firstValue("Content-Type");
        assertTrue("Expected Content-Type header to be set", contentType.isPresent());
        assertTrue("Expected UTF-8 charset in Content-Type: " + contentType.get(),
                contentType.get().toLowerCase().contains("charset=utf-8"));
    }
}
