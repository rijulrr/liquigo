package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
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
	defaultFeedURL = "wss://ws.kraken.com/v2"
	defaultPair    = "BTC/USD"
	defaultOutFile = "kraken_book.ndjson"
	defaultPprof   = "localhost:6060"
	reconnectDelay = 2 * time.Second
	readDeadline   = 30 * time.Second
	metricsEvery   = 5 * time.Second
)

type bookEvent struct {
	IngestedAtUTC time.Time       `json:"ingested_at_utc"`
	RawPayload    json.RawMessage `json:"raw_payload"`
}

func main() {
	runtime.MemProfileRate = 1

	cfg := readConfig()

	log.Printf("pprof listening on http://%s/debug/pprof/", cfg.pprofAddr)
	go func() {
		if err := http.ListenAndServe(cfg.pprofAddr, nil); err != nil {
			log.Printf("pprof server stopped: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runNaiveIngestLoop(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("stream stopped: %v", err)
	}
}

type config struct {
	feedURL   string
	pair      string
	outFile   string
	pprofAddr string
}

func readConfig() config {
	var cfg config
	flag.StringVar(&cfg.feedURL, "feed-url", envOrDefault("KRAKEN_FEED_URL", defaultFeedURL), "Kraken websocket URL")
	flag.StringVar(&cfg.pair, "pair", envOrDefault("KRAKEN_PAIR", defaultPair), "Trading pair to subscribe to")
	flag.StringVar(&cfg.outFile, "out-file", envOrDefault("OUT_FILE", defaultOutFile), "NDJSON output file")
	flag.StringVar(&cfg.pprofAddr, "pprof-addr", envOrDefault("PPROF_ADDR", defaultPprof), "pprof HTTP bind address")
	flag.Parse()
	return cfg
}

func runNaiveIngestLoop(ctx context.Context, cfg config) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := streamOnce(ctx, cfg)
		if err == nil || errors.Is(err, context.Canceled) {
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
