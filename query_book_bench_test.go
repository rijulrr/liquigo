package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func BenchmarkBuildRows(b *testing.B) {
	store, _ := benchmarkStore(4096)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rows, err := buildRows(store.events)
		if err != nil {
			b.Fatalf("buildRows error: %v", err)
		}
		if len(rows) == 0 {
			b.Fatal("expected non-empty rows")
		}
	}
}

func BenchmarkRunQueryBook(b *testing.B) {
	store, q := benchmarkStore(4096)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		row, count, err := RunQueryBook(store, q)
		if err != nil {
			b.Fatalf("RunQueryBook error: %v", err)
		}
		if count == 0 {
			b.Fatal("expected non-empty results")
		}
		if row.BestBid == -1 || row.BestAsk == -1 {
			b.Fatal("expected valid book state")
		}
	}
}

func BenchmarkApplyLevels(b *testing.B) {
	b.Run("insert_update", func(b *testing.B) {
		levels := []bookLevel{
			{Price: 73745.6, Qty: 0.50000000},
			{Price: 73745.7, Qty: 0.70000000},
			{Price: 73745.8, Qty: 0.90000000},
			{Price: 73745.9, Qty: 1.10000000},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			side := make(map[Price]Qty, 8)
			best := Price(-1)
			applyLevels(side, &best, levels, true)
			if best == -1 {
				b.Fatal("expected best bid")
			}
		}
	})

	b.Run("delete_best_rescan", func(b *testing.B) {
		initial := []bookLevel{
			{Price: 73745.6, Qty: 0.50000000},
			{Price: 73745.7, Qty: 0.70000000},
			{Price: 73745.8, Qty: 0.90000000},
			{Price: 73745.9, Qty: 1.10000000},
		}
		deletes := []bookLevel{
			{Price: 73745.9, Qty: 0},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			side := make(map[Price]Qty, 8)
			best := Price(-1)
			applyLevels(side, &best, initial, true)
			applyLevels(side, &best, deletes, true)
			if best != scalePrice(73745.8) {
				b.Fatalf("got best=%d want=%d", best, scalePrice(73745.8))
			}
		}
	})
}

func benchmarkStore(eventCount int) (UpdateEventStore, query) {
	base := time.Date(2026, time.March, 16, 16, 47, 0, 0, time.UTC)
	events := make([]UpdateEvent, 0, eventCount)

	events = append(events, UpdateEvent{
		Ts: base,
		RawPayload: mustRawBench(map[string]any{
			"channel": "book",
			"type":    "snapshot",
			"data": []any{map[string]any{
				"symbol": "BTC/USD",
				"bids": []any{
					map[string]any{"price": 73745.6, "qty": 1.00000000},
					map[string]any{"price": 73745.5, "qty": 1.25000000},
					map[string]any{"price": 73745.4, "qty": 1.50000000},
				},
				"asks": []any{
					map[string]any{"price": 73745.7, "qty": 1.00000000},
					map[string]any{"price": 73745.8, "qty": 1.25000000},
					map[string]any{"price": 73745.9, "qty": 1.50000000},
				},
			}},
		}),
	})

	for i := 1; i < eventCount; i++ {
		events = append(events, UpdateEvent{
			Ts:         base.Add(time.Duration(i) * time.Millisecond),
			RawPayload: benchmarkUpdatePayload(i),
		})
	}

	start := base.Add(time.Duration(eventCount/4) * time.Millisecond)
	end := base.Add(time.Duration((eventCount*3)/4) * time.Millisecond)
	return UpdateEventStore{events: events}, query{Start: start, End: end}
}

func benchmarkUpdatePayload(i int) json.RawMessage {
	bidPrice := 73745.0 + float64((i%10))/10
	askPrice := 73745.7 + float64((i%10))/10
	bidQty := 0.5 + float64(i%7)/10
	askQty := 0.6 + float64(i%11)/10

	bids := []any{
		map[string]any{"price": bidPrice, "qty": bidQty},
	}
	asks := []any{
		map[string]any{"price": askPrice, "qty": askQty},
	}

	if i%64 == 0 {
		bids = []any{map[string]any{"price": 73745.9, "qty": 0.0}}
	}
	if i%96 == 0 {
		asks = []any{map[string]any{"price": 73745.7, "qty": 0.0}}
	}

	return mustRawBench(map[string]any{
		"channel": "book",
		"type":    "update",
		"data": []any{map[string]any{
			"symbol": fmt.Sprintf("BTC/USD-%d", i%2),
			"bids":   bids,
			"asks":   asks,
		}},
	})
}

func mustRawBench(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return json.RawMessage(b)
}
