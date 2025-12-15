# RWG (Request Workload Generator)

RWG is a high-performance HTTP/gRPC load generator designed for precise request scheduling and latency measurement.

## Recent Optimizations (High Performance Mode)

Significant architectural changes have been made to support high concurrency (e.g., 5000+ workers) with minimal overhead and "cold start" latency.

### 1. Shared HTTP Transport & Connection Pooling
*   **Problem**: Previously, each worker created its own `http.Transport`, forcing a separate TCP connection handshake for every single worker. With 2000+ workers, this caused a massive "connection storm" at startup, leading to high tail latency (P99/Max) and dropped packets.
*   **Solution**: All workers now share a **single `http.Client`** instance.
*   **Result**: This enables efficient connection pooling. `MaxIdleConns` and `MaxIdleConnsPerHost` are automatically tuned to match the number of workers, ensuring established connections are reused across requests, matching the behavior of tools like `vegeta`.

### 2. Thread-Safe Buffer Pooling
*   **Problem**: High throughput generates massive garbage collection (GC) pressure due to allocating a 32KB buffer for every request to drain the response body.
*   **Solution**: Implemented a global **`sync.Pool`** for IO buffers.
*   **Result**: Buffers are reused across requests and workers in a thread-safe manner (locking handled by `sync.Pool`). This drastically reduces memory allocations and GC pause times.

### 3. Asynchronous Streaming Collector
*   **Problem**: The `Collector` previously stored every `Sample` struct (containing pointers) in a massive slice. As the test ran, the live heap grew to gigabytes, causing GC scan phases to degrade CPU performance and increase latency over time.
*   **Solution**:
    *   **Streaming**: Samples are written to the output CSV immediately in batches of 1000.
    *   **Async I/O**: A dedicated goroutine handles disk I/O so the main collection loop is never blocked by filesystem latency.
    *   **Zero-Alloc Stats**: The collector now only keeps `[]int64` latencies in memory for final stat calculation, removing pointer overhead from the heap.
    *   **Batch Pools**: Batch slices are also recycled via `sync.Pool`.

### 4. TCP Keep-Alive
*   **Feature**: `net.Dialer` is configured with a 30-second TCP Keep-Alive to prevent intermediate disconnects during long tests or low-rate phases.