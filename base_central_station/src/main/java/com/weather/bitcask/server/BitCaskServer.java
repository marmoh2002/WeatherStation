package com.weather.bitcask.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.weather.bitcask.engine.BitCaskStore;
import com.weather.model.StatusMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BitCaskServer {
    private static final Logger logger = LogManager.getLogger(BitCaskServer.class);
    private static final int MIN_CORE_THREADS = 4;
    private static final int MIN_MAX_THREADS = 16;
    private static final int WORKER_QUEUE_CAPACITY = 1024;
    private static final long KEEP_ALIVE_SECONDS = 60L;

    private final BitCaskStore store;
    private HttpServer server;
    private ExecutorService executor;

    public BitCaskServer(BitCaskStore store, int port) {
        this(
                store,
                port,
                defaultPoolSize(MIN_CORE_THREADS, 2),
                defaultPoolSize(MIN_MAX_THREADS, 4),
                WORKER_QUEUE_CAPACITY);
    }

    BitCaskServer(BitCaskStore store, int port, int corePoolSize, int maxPoolSize, int queueCapacity) {
        this.store = store;
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
            this.server.createContext("/view-all", new ViewAllHandler());
            this.server.createContext("/view-key", new ViewKeyHandler());
            AtomicInteger workerId = new AtomicInteger(1);
            ThreadFactory threadFactory = runnable -> {
                Thread thread = new Thread(runnable, "bitcask-http-worker-" + workerId.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            };
            this.executor = new ThreadPoolExecutor(
                    corePoolSize,
                    maxPoolSize,
                    KEEP_ALIVE_SECONDS,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(queueCapacity),
                    threadFactory,
                    new ThreadPoolExecutor.AbortPolicy());
            this.server.setExecutor(this.executor);
        } catch (IOException e) {
            logger.error("Failed to start HTTP Server", e);
        }
    }

    private static int defaultPoolSize(int minThreads, int cpuMultiplier) {
        return Math.max(minThreads, Runtime.getRuntime().availableProcessors() * cpuMultiplier);
    }

    public void start() {
        if (server != null) {
            server.start();
            logger.info("BitCask API Server started on port " + server.getAddress().getPort());
        }
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            logger.info("BitCask API Server stopped.");
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    public int getPort() {
        return server != null ? server.getAddress().getPort() : -1;
    }

    ExecutorService getExecutor() {
        return executor;
    }

    // Handles requests to get ALL keys
    private class ViewAllHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                StringBuilder response = new StringBuilder();
                Set<Long> keys = store.getInMemoryIndex().keySet();

                for (Long key : keys) {
                    Optional<StatusMessage> msg = store.get(key);
                    // Format as CSV line: key, value
                    msg.ifPresent(statusMessage -> response.append(key).append(",").append(statusMessage.toString())
                            .append("\n"));
                }

                sendResponse(exchange, 200, response.toString());
            } else {
                sendResponse(exchange, 405, "Method Not Allowed");
            }
        }
    }

    // Handles requests to get a single key
    private class ViewKeyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }

            try {
                String idParam = extractQueryParam(exchange.getRequestURI(), "id");
                if (idParam == null || idParam.isEmpty()) {
                    sendResponse(exchange, 400, "Missing ID parameter");
                    return;
                }

                long id = Long.parseLong(idParam);
                Optional<StatusMessage> msg = store.get(id);

                if (msg.isPresent()) {
                    sendResponse(exchange, 200, msg.get().toString());
                } else {
                    sendResponse(exchange, 404, "Key Not Found");
                }
            } catch (IllegalArgumentException e) {
                sendResponse(exchange, 400, "Invalid ID format");
            }
        }
    }

    private static String extractQueryParam(URI uri, String targetKey) {
        String rawQuery = uri.getRawQuery();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return null;
        }
        for (String pair : rawQuery.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eqIdx = pair.indexOf('=');
            String rawName = (eqIdx >= 0) ? pair.substring(0, eqIdx) : pair;
            String name = URLDecoder.decode(rawName, StandardCharsets.UTF_8);
            if (targetKey.equals(name)) {
                if (eqIdx < 0) {
                    return "";
                }
                return URLDecoder.decode(pair.substring(eqIdx + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }
}