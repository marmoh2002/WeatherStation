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
import java.util.Optional;
import java.util.Set;

public class BitCaskServer {
    private static final Logger logger = LogManager.getLogger(BitCaskServer.class);
    private final BitCaskStore store;
    private HttpServer server;

    public BitCaskServer(BitCaskStore store, int port) {
        this.store = store;
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
            this.server.createContext("/view-all", new ViewAllHandler());
            this.server.createContext("/view-key", new ViewKeyHandler());
            this.server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        } catch (IOException e) {
            logger.error("Failed to start HTTP Server", e);
        }
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
            if ("GET".equals(exchange.getRequestMethod())) {
                String query = exchange.getRequestURI().getQuery();
                if (query != null && query.startsWith("id=")) {
                    try {
                        long id = Long.parseLong(query.split("=")[1]);
                        Optional<StatusMessage> msg = store.get(id);

                        if (msg.isPresent()) {
                            sendResponse(exchange, 200, msg.get().toString());
                        } else {
                            sendResponse(exchange, 404, "Key Not Found");
                        }
                    } catch (NumberFormatException e) {
                        sendResponse(exchange, 400, "Invalid ID format");
                    }
                } else {
                    sendResponse(exchange, 400, "Missing ID parameter");
                }
            } else {
                sendResponse(exchange, 405, "Method Not Allowed");
            }
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] responseBytes = response.getBytes();
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        OutputStream os = exchange.getResponseBody();
        os.write(responseBytes);
        os.close();
    }
}