package org.apache.flink.connector.clickhouse.token;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Single-route HTTP stub that records the last request. Test helper. */
final class StubHttpServer implements AutoCloseable {

    private final HttpServer server;
    volatile String lastBody;
    volatile String lastPath;
    volatile String lastContentType;
    private volatile int status = 200;
    private volatile String responseBody = "";
    private volatile long headerDelayMillis;
    private volatile long bodyStallMillis;
    private final CountDownLatch closed = new CountDownLatch(1);
    private final ConcurrentLinkedQueue<Object[]> queued = new ConcurrentLinkedQueue<>();
    final AtomicInteger requests = new AtomicInteger();

    StubHttpServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    void respond(int status, String body) {
        this.status = status;
        this.responseBody = body;
    }

    /** Answers the next request with this response; once the queue is empty, {@link #respond} applies. */
    void enqueue(int status, String body) {
        queued.add(new Object[] {status, body});
    }

    /** Waits this long (or until close) before sending the response headers. */
    void delayHeaders(long millis) {
        this.headerDelayMillis = millis;
    }

    /** Sends the headers and half of the body, then waits this long (or until close) before the rest. */
    void stallBody(long millis) {
        this.bodyStallMillis = millis;
    }

    URI uri(String path) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
    }

    private void handle(HttpExchange exchange) throws IOException {
        lastBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        lastContentType = exchange.getRequestHeaders().getFirst("Content-Type");
        lastPath = exchange.getRequestURI().getPath();
        requests.incrementAndGet();
        Object[] next = queued.poll();
        int status = next != null ? (Integer) next[0] : this.status;
        byte[] out = (next != null ? (String) next[1] : responseBody).getBytes(StandardCharsets.UTF_8);
        pause(headerDelayMillis);
        exchange.sendResponseHeaders(status, out.length == 0 ? -1 : out.length);
        if (out.length > 0) {
            try (OutputStream os = exchange.getResponseBody()) {
                if (bodyStallMillis > 0) {
                    os.write(out, 0, out.length / 2);
                    os.flush();
                    pause(bodyStallMillis);
                    os.write(out, out.length / 2, out.length - out.length / 2);
                } else {
                    os.write(out);
                }
            } catch (IOException e) {
                // client gave up (timeout test)
            }
        }
        exchange.close();
    }

    private void pause(long millis) {
        try {
            closed.await(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        closed.countDown();
        server.stop(0);
    }
}
