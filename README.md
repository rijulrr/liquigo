# liquigo 

Lightweight Kraken websocket ingestor that writes orderbook stream messages to NDJSON.

## What It Does

- Connects to `wss://ws.kraken.com/v2`
- Subscribes to `book` channel for one trading pair (default `BTC/USD`)
- Appends each websocket payload to NDJSON with an ingestion timestamp
- Flushes/syncs periodically
- Exposes `pprof` for profiling
- Logs ingest rates every 5 seconds:
  - `msgs/sec`
  - `bytes/sec`
  - `buffer usage`

## Output Format

Each output line is a JSON object:

```json
{
  "ingested_at_utc": "2026-03-15T19:43:10.123456Z",
  "raw_payload": { "...": "original Kraken message" }
}
```

Default output file: `kraken_book.ndjson`

## Run

```bash
go run .
```

### Flags

```bash
go run . \
  -feed-url wss://ws.kraken.com/v2 \
  -pair BTC/USD \
  -out-file kraken_book.ndjson \
  -pprof-addr localhost:6060
```

### Environment Variables

- `KRAKEN_FEED_URL`
- `KRAKEN_PAIR`
- `OUT_FILE`
- `PPROF_ADDR`

## Metrics Logs

Every 5 seconds, logs look like:

```text
msgs/sec=1234.56 bytes/sec=987654.32 buffer usage=12000/1048576 (1.1%)
```

## Profiling

pprof endpoints:

- `http://localhost:6060/debug/pprof/`
- `http://localhost:6060/debug/pprof/heap`
- `http://localhost:6060/debug/pprof/profile?seconds=15`

Examples:

```bash
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=15
go tool pprof http://localhost:6060/debug/pprof/heap
curl -o cpu.pprof "http://localhost:6060/debug/pprof/profile?seconds=15"
go tool pprof cpu.pprof
```
