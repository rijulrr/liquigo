package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	modeIngest = "ingest"
	modeReplay = "replay"

	defaultFeedURL = "wss://ws.kraken.com/v2"
	defaultPair    = "BTC/USD"
	defaultOutFile = "kraken_book.ndjson"
	defaultInFile  = "kraken_book.ndjson"
	defaultPprof   = "localhost:6060"
	defaultMode    = modeIngest
	defaultSpeed   = 1.0

	reconnectDelay = 2 * time.Second
	readDeadline   = 30 * time.Second
	metricsEvery   = 5 * time.Second

	flushThresholdBytes = 1 << 20 // 1MB
	syncInterval        = 2 * time.Second
)

type bookEvent struct {
	IngestedAtUTC time.Time       `json:"ingested_at_utc"`
	RawPayload    json.RawMessage `json:"raw_payload"`
}

type config struct {
	mode      string
	feedURL   string
	pair      string
	outFile   string
	pprofAddr string
	inFile    string
	speed     float64
}

type ingestStats struct {
	lastSync    time.Time
	lastMetrics time.Time
	msgCount    int
	byteCount   int
}

type replayCadence struct {
	startWall   time.Time
	startSource time.Time
	lineStart   int
}

func main() {
	configureRuntimeProfiling()

	cfg, err := readConfig()
	if err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startPprofServer(cfg.pprofAddr)

	if err := runMode(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("%s stopped: %v", cfg.mode, err)
	}
}

func configureRuntimeProfiling() {
	// Decision: profile every allocation to make memory behavior explicit in this benchmark-style binary.
	runtime.MemProfileRate = 1
}

func startPprofServer(addr string) {
	log.Printf("pprof listening on http://%s/debug/pprof/", addr)
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("pprof server stopped: %v", err)
		}
	}()
}

func runMode(ctx context.Context, cfg config) error {
	// Decision: mode chooses a single top-level execution path to keep startup explicit.
	switch cfg.mode {
	case modeIngest:
		return runNaiveIngestLoop(ctx, cfg)
	case modeReplay:
		return runReplay(ctx, cfg)
	default:
		return fmt.Errorf("unsupported mode %q", cfg.mode)
	}
}

func readConfig() (config, error) {
	var cfg config
	flag.StringVar(&cfg.mode, "mode", defaultMode, "Run mode: ingest or replay")
	flag.StringVar(&cfg.feedURL, "feed-url", envOrDefault("KRAKEN_FEED_URL", defaultFeedURL), "Kraken websocket URL")
	flag.StringVar(&cfg.pair, "pair", envOrDefault("KRAKEN_PAIR", defaultPair), "Trading pair to subscribe to")
	flag.StringVar(&cfg.outFile, "out-file", envOrDefault("OUT_FILE", defaultOutFile), "NDJSON output file")
	flag.StringVar(&cfg.pprofAddr, "pprof-addr", envOrDefault("PPROF_ADDR", defaultPprof), "pprof HTTP bind address")
	flag.StringVar(&cfg.inFile, "in-file", defaultInFile, "NDJSON input file for replay mode")
	flag.Float64Var(&cfg.speed, "speed", defaultSpeed, "Replay speed multiplier (ignored in max-throughput replay mode)")
	flag.Parse()

	if err := validateMode(cfg.mode); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func validateMode(mode string) error {
	// Decision: fail fast at config parse time so unsupported modes never start runtime side effects.
	switch mode {
	case modeIngest, modeReplay:
		return nil
	default:
		return fmt.Errorf("mode must be %q or %q, got %q", modeIngest, modeReplay, mode)
	}
}

func runNaiveIngestLoop(ctx context.Context, cfg config) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := streamOnce(ctx, cfg)
		if err == nil {
			return nil
		}

		log.Printf("stream error: %v (reconnect in %s)", err, reconnectDelay)

		// Decision: reconnect with a fixed delay to avoid a tight retry loop while keeping logic simple.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectDelay):
		}
	}
}

func streamOnce(ctx context.Context, cfg config) error {
	file, writer, closeWriter, err := openBufferedOutput(cfg.outFile)
	if err != nil {
		return err
	}
	defer closeWriter()

	conn, err := connectAndSubscribe(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	stats := newIngestStats()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		payload, err := readPayload(conn)
		if err != nil {
			return err
		}

		if err := writeIngestEvent(writer, payload); err != nil {
			return err
		}

		stats.recordPayload(len(payload))

		if shouldFlushBuffer(writer) {
			// Decision: flush once buffered data crosses threshold so disk writes are batched without unbounded buffering.
			if err := writer.Flush(); err != nil {
				return err
			}
		}

		now := time.Now()
		if stats.shouldSync(now) {
			// Decision: periodic fsync bounds data-loss window during abrupt process termination.
			if err := syncFile(writer, file); err != nil {
				return err
			}
			stats.markSynced(now)
		}

		if stats.shouldReportMetrics(now) {
			// Decision: periodic aggregated metrics avoid per-message logging overhead.
			stats.logAndReset(writer, now)
		}
	}
}

func openBufferedOutput(path string) (*os.File, *bufio.Writer, func(), error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, nil, err
	}

	writer := bufio.NewWriterSize(file, flushThresholdBytes)
	closeFn := func() {
		if err := writer.Flush(); err != nil {
			log.Printf("flush error: %v", err)
		}
		if err := file.Close(); err != nil {
			log.Printf("close file error: %v", err)
		}
	}

	return file, writer, closeFn, nil
}

func connectAndSubscribe(ctx context.Context, cfg config) (*websocket.Conn, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, cfg.feedURL, nil)
	if err != nil {
		return nil, err
	}

	subscribeMsg := map[string]any{
		"method": "subscribe",
		"params": map[string]any{
			"channel": "book",
			"symbol":  []string{cfg.pair},
			"depth":   100,
		},
	}

	if err := conn.WriteJSON(subscribeMsg); err != nil {
		conn.Close()
		return nil, err
	}

	log.Printf("connected to %s for pair %s", cfg.feedURL, cfg.pair)
	return conn, nil
}

func readPayload(conn *websocket.Conn) ([]byte, error) {
	if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
		return nil, err
	}

	_, payload, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func writeIngestEvent(writer *bufio.Writer, payload []byte) error {
	evt := event(payload)
	encoded, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	if _, err := writer.Write(encoded); err != nil {
		return err
	}
	return writer.WriteByte('\n')
}

func shouldFlushBuffer(writer *bufio.Writer) bool {
	return writer.Buffered() >= flushThresholdBytes
}

func syncFile(writer *bufio.Writer, file *os.File) error {
	if err := writer.Flush(); err != nil {
		return err
	}
	return file.Sync()
}

func newIngestStats() *ingestStats {
	now := time.Now()
	return &ingestStats{lastSync: now, lastMetrics: now}
}

func (s *ingestStats) recordPayload(payloadBytes int) {
	s.msgCount++
	s.byteCount += payloadBytes
}

func (s *ingestStats) shouldSync(now time.Time) bool {
	return now.Sub(s.lastSync) > syncInterval
}

func (s *ingestStats) markSynced(now time.Time) {
	s.lastSync = now
}

func (s *ingestStats) shouldReportMetrics(now time.Time) bool {
	return now.Sub(s.lastMetrics) >= metricsEvery
}

func (s *ingestStats) logAndReset(writer *bufio.Writer, now time.Time) {
	elapsed := now.Sub(s.lastMetrics).Seconds()
	buffered := writer.Buffered()
	bufferCap := buffered + writer.Available()
	bufferPct := 0.0
	if bufferCap > 0 {
		bufferPct = (float64(buffered) / float64(bufferCap)) * 100
	}
	log.Printf("msgs/sec=%.2f bytes/sec=%.2f buffer usage=%d/%d (%.1f%%)",
		float64(s.msgCount)/elapsed,
		float64(s.byteCount)/elapsed,
		buffered,
		bufferCap,
		bufferPct,
	)

	s.lastMetrics = now
	s.msgCount = 0
	s.byteCount = 0
}

func runReplay(ctx context.Context, cfg config) error {
	return runReplayToWriter(ctx, cfg, os.Stdout)
}

func runReplayToWriter(ctx context.Context, cfg config, out io.Writer) error {
	lines, err := loadReplayLines(cfg.inFile)
	if err != nil {
		return err
	}

	if cfg.speed != 1 {
		// Decision: replay intentionally targets max-throughput profiling, so timestamp pacing is disabled.
		log.Printf("replay runs at max throughput; -speed is ignored (got %.2f)", cfg.speed)
	}

	lineNo := 0
	cadence := replayCadence{}

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		lineNo++

		if ctx.Err() != nil {
			return ctx.Err()
		}

		evt, err := decodeReplayEnvelope(lineNo, line)
		if err != nil {
			return err
		}

		if err := decodeReplayPayload(lineNo, evt.RawPayload); err != nil {
			return err
		}

		// if err := writeReplayPayload(out, lineNo, evt.RawPayload); err != nil {
		// 	return err
		// }

		cadence.observe(evt.IngestedAtUTC, lineNo)
	}

	log.Printf("replay completed lines=%d mode=max-throughput", lineNo)
	return nil
}

func loadReplayLines(path string) ([][]byte, error) {
	// Decision: read once and split to minimize per-line file I/O during CPU-focused replay.
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return bytes.Split(data, []byte{'\n'}), nil
}

func decodeReplayEnvelope(lineNo int, line []byte) (bookEvent, error) {
	var evt bookEvent
	if err := json.Unmarshal(line, &evt); err != nil {
		return bookEvent{}, fmt.Errorf("line %d: decode envelope: %w", lineNo, err)
	}

	// Decision: reject malformed envelopes early so downstream parsing and output are deterministic.
	if evt.IngestedAtUTC.IsZero() {
		return bookEvent{}, fmt.Errorf("line %d: missing ingested_at_utc", lineNo)
	}
	if len(evt.RawPayload) == 0 {
		return bookEvent{}, fmt.Errorf("line %d: missing raw_payload", lineNo)
	}

	return evt, nil
}

func decodeReplayPayload(lineNo int, raw json.RawMessage) error {
	// Decision: force an inner JSON decode during replay so profiling includes payload parsing cost.
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("line %d: decode payload: %w", lineNo, err)
	}
	return nil
}

func writeReplayPayload(out io.Writer, lineNo int, raw json.RawMessage) error {
	if _, err := out.Write(raw); err != nil {
		return fmt.Errorf("line %d: write raw payload: %w", lineNo, err)
	}
	if _, err := out.Write([]byte{'\n'}); err != nil {
		return fmt.Errorf("line %d: write newline: %w", lineNo, err)
	}
	return nil
}

func (c *replayCadence) observe(sourceTime time.Time, lineNo int) {
	if c.startWall.IsZero() {
		c.startWall = time.Now()
		c.startSource = sourceTime
		c.lineStart = lineNo
		return
	}

	now := time.Now()
	if now.Sub(c.startWall) < metricsEvery {
		return
	}

	wallElapsed := now.Sub(c.startWall).Seconds()
	sourceElapsed := sourceTime.Sub(c.startSource).Seconds()
	actualSpeed := 0.0
	if wallElapsed > 0 {
		actualSpeed = sourceElapsed / wallElapsed
	}

	log.Printf("replay cadence=%.2fx lines=%d (+%d in %.2fs)",
		actualSpeed,
		lineNo,
		lineNo-c.lineStart,
		wallElapsed,
	)

	c.startWall = now
	c.startSource = sourceTime
	c.lineStart = lineNo
}

func event(payload []byte) bookEvent {
	return bookEvent{
		IngestedAtUTC: time.Now().UTC(),
		RawPayload:    append([]byte(nil), payload...),
	}
}

func envOrDefault(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}
