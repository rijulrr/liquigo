package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunReplayOutputsRawPayloadAndCadence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.ndjson")

	base := time.Date(2026, time.March, 16, 16, 47, 0, 0, time.UTC)
	events := []bookEvent{
		{IngestedAtUTC: base, RawPayload: mustRaw(t, map[string]any{"channel": "status", "type": "update"})},
		{IngestedAtUTC: base.Add(100 * time.Millisecond), RawPayload: mustRaw(t, map[string]any{"method": "subscribe", "success": true})},
		{IngestedAtUTC: base.Add(400 * time.Millisecond), RawPayload: mustRaw(t, map[string]any{"channel": "book", "type": "snapshot"})},
	}

	encodedLines := make([]string, 0, len(events))
	for _, evt := range events {
		b, err := json.Marshal(evt)
		if err != nil {
			t.Fatalf("marshal test input: %v", err)
		}
		encodedLines = append(encodedLines, string(b))
	}

	input := strings.Join(encodedLines, "\n") + "\n"
	if err := os.WriteFile(path, []byte(input), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	cfg := config{inFile: path, speed: 2}
	var out strings.Builder
	if err := runReplayToWriter(context.Background(), cfg, &out); err != nil {
		t.Fatalf("runReplay failed: %v", err)
	}

	gotLines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(gotLines) != len(events) {
		t.Fatalf("got %d output lines, want %d", len(gotLines), len(events))
	}

	for i, line := range gotLines {
		if line != string(events[i].RawPayload) {
			t.Fatalf("line %d mismatch: got %s want %s", i+1, line, string(events[i].RawPayload))
		}
	}
}

func TestRunReplayMalformedLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.ndjson")

	content := "{bad-json}\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	cfg := config{inFile: path, speed: 1}
	err := runReplayToWriter(context.Background(), cfg, io.Discard)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("expected line-numbered error, got %v", err)
	}
}

func TestRunQueryModeToWriterPrintsBestBidAndAskOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.ndjson")

	base := time.Date(2026, time.March, 16, 16, 47, 0, 0, time.UTC)
	events := []bookEvent{
		{
			IngestedAtUTC: base,
			RawPayload: mustRaw(t, map[string]any{
				"channel": "book",
				"type":    "snapshot",
				"data": []any{map[string]any{
					"symbol": "BTC/USD",
					"bids":   []any{map[string]any{"price": 100.0, "qty": 1.0}},
					"asks":   []any{map[string]any{"price": 100.1, "qty": 2.0}},
				}},
			}),
		},
		{
			IngestedAtUTC: base.Add(time.Second),
			RawPayload: mustRaw(t, map[string]any{
				"channel": "book",
				"type":    "update",
				"data": []any{map[string]any{
					"symbol": "BTC/USD",
					"bids":   []any{map[string]any{"price": 100.2, "qty": 1.5}},
					"asks":   []any{},
				}},
			}),
		},
	}

	encodedLines := make([]string, 0, len(events))
	for _, evt := range events {
		b, err := json.Marshal(evt)
		if err != nil {
			t.Fatalf("marshal test input: %v", err)
		}
		encodedLines = append(encodedLines, string(b))
	}

	input := strings.Join(encodedLines, "\n") + "\n"
	if err := os.WriteFile(path, []byte(input), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	cfg := config{
		mode:   modeQuery,
		inFile: path,
		query: query{
			Start: base.Add(500 * time.Millisecond),
			End:   base.Add(1500 * time.Millisecond),
		},
	}

	var out strings.Builder
	if err := runQueryModeToWriter(context.Background(), cfg, &out); err != nil {
		t.Fatalf("runQueryMode failed: %v", err)
	}

	got := strings.TrimSpace(out.String())
	if got != "best_bid=100.2 best_ask=100.1" {
		t.Fatalf("query output mismatch: got %s", got)
	}
}

func TestParseQueryRange(t *testing.T) {
	got, err := parseQueryRange("2026-03-16T16:47:00Z,2026-03-16T16:47:01Z")
	if err != nil {
		t.Fatalf("parseQueryRange error: %v", err)
	}
	if got.Start.IsZero() || got.End.IsZero() {
		t.Fatalf("expected parsed range, got %+v", got)
	}
	if !got.Start.Before(got.End) {
		t.Fatalf("expected start before end, got %+v", got)
	}
}

func mustRaw(t *testing.T, v any) json.RawMessage {
	t.Helper()

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal raw payload: %v", err)
	}
	return json.RawMessage(b)
}
