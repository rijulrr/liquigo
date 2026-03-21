package main

import (
	"bufio"
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
)

type bookEvent struct {
	IngestedAtUTC time.Time       `json:"ingested_at_utc"`
	RawPayload    json.RawMessage `json:"raw_payload"`
}

type waitFunc func(context.Context, time.Duration) error

func main() {
	runtime.MemProfileRate = 1

	cfg, err := readConfig()
	if err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("pprof listening on http://%s/debug/pprof/", cfg.pprofAddr)
	go func() {
		if err := http.ListenAndServe(cfg.pprofAddr, nil); err != nil {
			log.Printf("pprof server stopped: %v", err)
		}
	}()

	switch cfg.mode {
	case modeIngest:
		if err := runNaiveIngestLoop(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("stream stopped: %v", err)
		}
	case modeReplay:
		if err := runReplay(ctx, cfg, os.Stdout, waitForDuration); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("replay stopped: %v", err)
		}
	default:
		log.Fatalf("unsupported mode %q", cfg.mode)
	}
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

func readConfig() (config, error) {
	var cfg config
	flag.StringVar(&cfg.mode, "mode", defaultMode, "Run mode: ingest or replay")
	flag.StringVar(&cfg.feedURL, "feed-url", envOrDefault("KRAKEN_FEED_URL", defaultFeedURL), "Kraken websocket URL")
	flag.StringVar(&cfg.pair, "pair", envOrDefault("KRAKEN_PAIR", defaultPair), "Trading pair to subscribe to")
	flag.StringVar(&cfg.outFile, "out-file", envOrDefault("OUT_FILE", defaultOutFile), "NDJSON output file")
	flag.StringVar(&cfg.pprofAddr, "pprof-addr", envOrDefault("PPROF_ADDR", defaultPprof), "pprof HTTP bind address")
	flag.StringVar(&cfg.inFile, "in-file", defaultInFile, "NDJSON input file for replay mode")
	flag.Float64Var(&cfg.speed, "speed", defaultSpeed, "Replay speed multiplier (>0), only used in replay mode")
	flag.Parse()

	switch cfg.mode {
	case modeIngest, modeReplay:
	default:
		return config{}, fmt.Errorf("mode must be %q or %q, got %q", modeIngest, modeReplay, cfg.mode)
	}

	if cfg.speed <= 0 {
		return config{}, fmt.Errorf("speed must be > 0, got %.4f", cfg.speed)
	}

	return cfg, nil
}

func runNaiveIngestLoop(ctx context.Context, cfg config) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := streamOnce(ctx, cfg)
		if err == nil {
			return err
		}

		log.Printf("stream error: %v (reconnect in %s)", err, reconnectDelay)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectDelay):
		}
	}
}

func streamOnce(ctx context.Context, cfg config) error {
	file, err := os.OpenFile(cfg.outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, 1<<20) // 1MB buffer
	defer func() {
		if err := writer.Flush(); err != nil {
			log.Printf("flush error: %v", err)
		}
	}()

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, cfg.feedURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	subscribeMsg := map[string]any{
		"method": "subscribe",
		"params": map[string]any{
			"channel": "book",
			"symbol":  []string{cfg.pair},
			"depth":   100,
		},
	}

	if err := conn.WriteJSON(subscribeMsg); err != nil {
		return err
	}

	log.Printf("connected to %s for pair %s", cfg.feedURL, cfg.pair)

	flushThreshold := 1 << 20 // 1MB
	lastSync := time.Now()
	lastMetrics := time.Now()
	msgCount := 0
	byteCount := 0

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
			return err
		}

		_, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		msgCount++
		byteCount += len(payload)

		evt := event(payload)

		encoded, err := json.Marshal(evt)
		if err != nil {
			return err
		}

		if _, err := writer.Write(encoded); err != nil {
			return err
		}

		if err := writer.WriteByte('\n'); err != nil {
			return err
		}

		// flush if buffer large enough
		if writer.Buffered() >= flushThreshold {
			if err := writer.Flush(); err != nil {
				return err
			}
		}

		// periodic fsync
		if time.Since(lastSync) > 2*time.Second {
			if err := writer.Flush(); err != nil {
				return err
			}

			if err := file.Sync(); err != nil {
				return err
			}

			lastSync = time.Now()
		}

		if time.Since(lastMetrics) >= metricsEvery {
			elapsed := time.Since(lastMetrics).Seconds()
			buffered := writer.Buffered()
			bufferCap := buffered + writer.Available()
			bufferPct := 0.0
			if bufferCap > 0 {
				bufferPct = (float64(buffered) / float64(bufferCap)) * 100
			}
			log.Printf("msgs/sec=%.2f bytes/sec=%.2f buffer usage=%d/%d (%.1f%%)",
				float64(msgCount)/elapsed,
				float64(byteCount)/elapsed,
				buffered,
				bufferCap,
				bufferPct,
			)
			lastMetrics = time.Now()
			msgCount = 0
			byteCount = 0
		}
	}
}

func runReplay(ctx context.Context, cfg config, out io.Writer, wait waitFunc) error {
	file, err := os.Open(cfg.inFile)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 128*1024*1024)

	lineNo := 0
	var prevTime time.Time

	for scanner.Scan() {
		lineNo++

		if ctx.Err() != nil {
			return ctx.Err()
		}

		var evt bookEvent
		if err := json.Unmarshal(scanner.Bytes(), &evt); err != nil {
			return fmt.Errorf("line %d: decode envelope: %w", lineNo, err)
		}

		if evt.IngestedAtUTC.IsZero() {
			return fmt.Errorf("line %d: missing ingested_at_utc", lineNo)
		}

		if len(evt.RawPayload) == 0 {
			return fmt.Errorf("line %d: missing raw_payload", lineNo)
		}

		if !prevTime.IsZero() {
			delay, err := replayDelay(prevTime, evt.IngestedAtUTC, cfg.speed)
			if err != nil {
				return fmt.Errorf("line %d: %w", lineNo, err)
			}
			if err := wait(ctx, delay); err != nil {
				return err
			}
		}

		if _, err := out.Write(evt.RawPayload); err != nil {
			return fmt.Errorf("line %d: write payload: %w", lineNo, err)
		}
		if _, err := out.Write([]byte{'\n'}); err != nil {
			return fmt.Errorf("line %d: write newline: %w", lineNo, err)
		}

		prevTime = evt.IngestedAtUTC
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func replayDelay(prev, next time.Time, speed float64) (time.Duration, error) {
	if speed <= 0 {
		return 0, fmt.Errorf("speed must be > 0, got %.4f", speed)
	}

	delta := next.Sub(prev)
	if delta < 0 {
		return 0, fmt.Errorf("timestamps not monotonic: %s then %s", prev.Format(time.RFC3339Nano), next.Format(time.RFC3339Nano))
	}
	if delta == 0 {
		return 0, nil
	}

	scaled := time.Duration(float64(delta) / speed)
	if scaled < 0 {
		return 0, nil
	}
	return scaled, nil
}

func waitForDuration(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
