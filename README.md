# RWG (Request Workload Generator)

RWG is a high-performance HTTP load generator designed for precise request scheduling and latency measurement. It generates workloads with configurable request rates and distribution patterns (fixed or exponential), then collects detailed latency metrics for performance analysis.

## Overview

RWG is built to support high-concurrency load testing (tested with 5000+ concurrent workers) with minimal overhead. The tool follows an open-loop architecture, meaning it schedules requests at precise intervals regardless of response times, making it ideal for measuring system behavior under realistic traffic patterns.

Key design goals:
- **Precise request scheduling**: Support for fixed-rate and exponential (Poisson) distributions
- **Low overhead**: Optimized for high throughput with minimal impact on measurements
- **Detailed metrics**: Microsecond-precision latency tracking with percentile analysis
- **CSV export**: Raw sample data for custom analysis and visualization

## Features

### Load Generation
- **Multiple distribution types**: Fixed-rate (constant intervals) and exponential (Poisson process)
- **Multi-phase testing**: Configure multiple rate/duration phases in a single run
- **Dynamic worker scaling**: Automatically scales worker pool based on demand
- **Configurable timeouts**: Per-request timeout control

### Performance Optimizations
- **Shared HTTP transport**: Single connection pool shared across all workers
- **Buffer pooling**: Thread-safe buffer reuse via `sync.Pool` to reduce GC pressure
- **Streaming CSV export**: Asynchronous batch writing prevents I/O blocking
- **TCP keep-alive**: 30-second keep-alive prevents connection drops during long tests

### Metrics Collection
- **Real-time statistics**: P50, P95, P99, P99.9 latency percentiles
- **Error tracking**: Timeout and error rate monitoring
- **Status code tracking**: Per-status-code request counts
- **CSV export**: Full sample data with timestamps for post-processing

### Result Analysis
- **Python analyzer**: Post-process results with warmup/cooldown trimming
- **Overall report**: JSON summary with goodput, SLO violations, latency percentiles
- **Real-time report**: Time-series CSV for plotting performance over time

## Installation

### Prerequisites
- **Go**: 1.24.6 or later
- **Python 3**: (optional) for result analysis with `analyzer.py`
- **Python packages**: (optional) install with `pip install -r requirements.txt`

### Building from Source

Clone the repository and build the binary:

```bash
git clone https://github.com/farzad1132/rwg.git
cd rwg
go build -o rwg
```

This creates the `rwg` binary in the current directory.

## Usage

### Basic Load Test

Run a fixed-rate test with 100 requests/second for 30 seconds using 500 workers:

```bash
./rwg run \
  --url https://example.com/api/endpoint \
  --dist fixed \
  --rates 100 \
  --durations 30 \
  --workers 500 \
  --output results.csv
```

### Multi-Phase Test

Run a test with three phases at different rates:

```bash
./rwg run \
  --url https://example.com/api \
  --dist fixed \
  --rates 50,100,200 \
  --durations 20,30,20 \
  --workers 1000 \
  --output results.csv
```

This runs:
1. 50 req/s for 20 seconds
2. 100 req/s for 30 seconds
3. 200 req/s for 20 seconds

### Exponential Distribution (Poisson Process)

Use exponential inter-arrival times for more realistic traffic:

```bash
./rwg run \
  --url https://example.com/api \
  --dist exp \
  --rates 100 \
  --durations 60 \
  --workers 500 \
  --output results.csv
```

### Additional Options

```bash
./rwg run \
  --url https://example.com/api \
  --dist fixed \
  --rates 100 \
  --durations 30 \
  --workers 500 \
  --output results.csv \
  --timeout 10 \              # Request timeout in seconds (default: 5)
  --ignore-errors \           # Continue on errors instead of stopping
  --stats=false               # Disable final statistics output
```

### Analyzing Results

Use the built-in analyzer to compute statistics with warmup/cooldown trimming:

```bash
./rwg parse \
  --rwg_output results.csv \
  --overall_output summary.json \
  --warmup 5 \
  --cooldown 5 \
  --version 1 \
  --slo 100
```

Parameters:
- `--warmup`: Seconds to trim from start (default: 0)
- `--cooldown`: Seconds to trim from end (default: 0)
- `--version`: HTTP version (1 or 2) for status code interpretation
- `--slo`: SLO threshold in milliseconds for goodput calculation

Generate a time-series report:

```bash
./rwg parse \
  --rwg_output results.csv \
  --realtime_output timeseries.csv \
  --freq 1000 \
  --warmup 5 \
  --cooldown 5 \
  --version 1 \
  --slo 100
```

The `--freq` parameter sets the interval in milliseconds for aggregating metrics.

### Check Version

```bash
./rwg version
```

## Project Structure

```
.
├── cmd/                    # Cobra CLI command implementations
│   ├── root.go            # Root command and CLI setup
│   ├── run.go             # Load generator implementation
│   ├── parse.go           # Analyzer wrapper command
│   └── version.go         # Version command
├── main.go                # Entry point
├── analyzer.py            # Python script for post-processing results
├── requirements.txt       # Python dependencies for analyzer
├── protobuf/              # Protocol buffer definitions (gRPC support - currently disabled)
│   ├── *.proto           # Proto source files
│   └── *.pb.go           # Generated Go code
├── testgrpcclient/        # Example gRPC client (for testing)
├── testgrpcserver/        # Example gRPC server (for testing)
├── testserver/            # Simple HTTP test server
├── go.mod                 # Go module definition
├── go.sum                 # Go dependency checksums
└── LICENSE                # MIT License
```

### Key Files

- **`cmd/run.go`**: Core load generation logic including worker pool, transport layer, scheduler, and collector
- **`analyzer.py`**: Python script for computing statistics, SLO violations, and percentiles from CSV output
- **`testserver/main.go`**: Minimal HTTP server that can be used as a test target

## Output Format

### CSV Output

The `--output` file contains one row per request with the following columns:

| Column | Description |
|--------|-------------|
| `url` | Target URL |
| `latency` | Request latency in microseconds |
| `status_code` | HTTP status code (0 indicates error) |
| `error` | Error message (empty for successful requests) |
| `timestamp` | Request start time in RFC3339Nano format |
| `current_workers` | Worker pool size when request was made |

### Console Statistics

At the end of each test run (unless `--stats=false`), RWG prints:

- Skipped iterations (requests that couldn't be scheduled due to backpressure)
- Request counts per status code
- Success rate (requests/second)
- Latency percentiles (P50, P95, P99, P99.9) in microseconds
- Min/max latency
- Total requests and test duration
- Maximum workers used from the pool

### Analyzer Output

The `parse` command generates JSON reports with:

- **Overall report**: Goodput, SLO violations, dropped requests, errors, throughput, latency percentiles, duration
- **Real-time report**: Time-series CSV with per-interval metrics for visualization

## Performance Characteristics

RWG has been optimized for high-concurrency scenarios:

- **Connection pooling**: Shared HTTP transport with configurable idle connection limits
- **Memory efficiency**: Buffer and batch pooling to minimize garbage collection
- **Async I/O**: Non-blocking CSV writes prevent disk I/O from affecting measurements
- **Auto-scaling workers**: Pool grows dynamically when utilization exceeds 70%

Tested configurations:
- 5000+ concurrent workers
- Sub-millisecond P99 overhead for the generator itself
- Sustained 10,000+ req/s on modern hardware

## License

MIT License - see [LICENSE](LICENSE) for details.

Copyright (c) 2026 Farzad Mohammadi and the Roshanfer authors
