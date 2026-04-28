package main

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"
)

type Price int64 // scale 1e1
type Qty int64   // scale 1e8

type Row struct {
	Ts      int64
	BestBid Price
	BestAsk Price
	Mid     Price
	Spread  Price
}

type Book struct {
	bids map[Price]Qty
	asks map[Price]Qty

	bestBid Price
	bestAsk Price
}

type UpdateEvent struct {
	Ts         time.Time
	RawPayload json.RawMessage
}

type UpdateEventStore struct {
	events []UpdateEvent
}

type query struct {
	Start   time.Time
	End     time.Time
	StartTs int64
	EndTs   int64
}

func RunQueryBook(store UpdateEventStore, query query) (Row, int, error) {
	rows, err := buildRows(store.events)
	if err != nil {
		return Row{}, 0, err
	}

	start, end, err := queryBounds(query)
	if err != nil {
		return Row{}, 0, err
	}
	if end < start {
		return Row{}, 0, nil
	}

	startIdx := sort.Search(len(rows), func(i int) bool {
		return rows[i].Ts >= start
	})
	endIdx := sort.Search(len(rows), func(i int) bool {
		return rows[i].Ts > end
	})
	if startIdx >= endIdx {
		return Row{}, 0, nil
	}

	last := rows[endIdx-1].Row
	return last, endIdx - startIdx, nil
}

type indexedRow struct {
	Row
	RawPayload json.RawMessage
}

type bookPayload struct {
	Channel string         `json:"channel"`
	Type    string         `json:"type"`
	Data    []bookSnapshot `json:"data"`
}

type bookSnapshot struct {
	Bids []bookLevel `json:"bids"`
	Asks []bookLevel `json:"asks"`
}

type bookLevel struct {
	Price float64 `json:"price"`
	Qty   float64 `json:"qty"`
}

func buildRows(events []UpdateEvent) ([]indexedRow, error) {
	book := Book{
		bids:    make(map[Price]Qty),
		asks:    make(map[Price]Qty),
		bestBid: -1,
		bestAsk: -1,
	}

	rows := make([]indexedRow, 0, len(events))
	for i, evt := range events {
		var payload bookPayload
		if err := json.Unmarshal(evt.RawPayload, &payload); err != nil {
			return nil, fmt.Errorf("event %d: decode payload: %w", i, err)
		}
		if payload.Channel != "book" {
			continue
		}
		if payload.Type != "snapshot" && payload.Type != "update" {
			continue
		}
		if len(payload.Data) == 0 {
			continue
		}

		for _, entry := range payload.Data {
			applyLevels(book.bids, &book.bestBid, entry.Bids, true)
			applyLevels(book.asks, &book.bestAsk, entry.Asks, false)

			if book.bestBid == -1 || book.bestAsk == -1 {
				continue
			}

			rawCopy := append(json.RawMessage(nil), evt.RawPayload...)
			rows = append(rows, indexedRow{
				Row: Row{
					Ts:      evt.Ts.UnixNano(),
					BestBid: book.bestBid,
					BestAsk: book.bestAsk,
					Mid:     (book.bestBid + book.bestAsk) / 2,
					Spread:  book.bestAsk - book.bestBid,
				},
				RawPayload: rawCopy,
			})
		}
	}

	return rows, nil
}

func applyLevels(side map[Price]Qty, best *Price, levels []bookLevel, isBid bool) {
	for _, level := range levels {
		price := scalePrice(level.Price)
		qty := scaleQty(level.Qty)

		if qty == 0 {
			delete(side, price)
			if price == *best {
				*best = rescanBest(side, isBid)
			}
			continue
		}

		side[price] = qty
		if *best == -1 {
			*best = price
			continue
		}
		if isBid && price > *best {
			*best = price
		}
		if !isBid && price < *best {
			*best = price
		}
	}
}

func rescanBest(side map[Price]Qty, isBid bool) Price {
	if len(side) == 0 {
		return -1
	}

	best := Price(-1)
	for price := range side {
		if best == -1 {
			best = price
			continue
		}
		if isBid && price > best {
			best = price
		}
		if !isBid && price < best {
			best = price
		}
	}
	return best
}

func scalePrice(v float64) Price {
	return Price(math.Round(v * 10))
}

func scaleQty(v float64) Qty {
	return Qty(math.Round(v * 1e8))
}

func queryBounds(q any) (int64, int64, error) {
	start, ok, err := extractQueryBound(q, []string{"Start", "From", "StartTs", "FromTs", "T1", "After"})
	if err != nil {
		return 0, 0, fmt.Errorf("query start: %w", err)
	}
	if !ok {
		start = math.MinInt64
	}

	end, ok, err := extractQueryBound(q, []string{"End", "To", "EndTs", "ToTs", "T2", "Before"})
	if err != nil {
		return 0, 0, fmt.Errorf("query end: %w", err)
	}
	if !ok {
		end = math.MaxInt64
	}

	return start, end, nil
}

func extractQueryBound(q any, fieldNames []string) (int64, bool, error) {
	v := reflect.ValueOf(q)
	if !v.IsValid() {
		return 0, false, nil
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, false, nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return 0, false, fmt.Errorf("unsupported query type %T", q)
	}

	for _, name := range fieldNames {
		field := v.FieldByName(name)
		if !field.IsValid() {
			continue
		}

		value, set, err := toUnixNano(field)
		if err != nil {
			return 0, false, fmt.Errorf("field %s: %w", name, err)
		}
		if !set {
			continue
		}
		return value, true, nil
	}

	return 0, false, nil
}

func toUnixNano(v reflect.Value) (int64, bool, error) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, false, nil
		}
		return toUnixNano(v.Elem())
	}

	if v.Type() == reflect.TypeOf(time.Time{}) {
		ts := v.Interface().(time.Time)
		if ts.IsZero() {
			return 0, false, nil
		}
		return ts.UnixNano(), true, nil
	}

	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v.Int() == 0 {
			return 0, false, nil
		}
		return v.Int(), true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := v.Uint()
		if u == 0 {
			return 0, false, nil
		}
		if u > math.MaxInt64 {
			return 0, false, fmt.Errorf("timestamp overflows int64")
		}
		return int64(u), true, nil
	default:
		return 0, false, fmt.Errorf("unsupported type %s", v.Type())
	}
}
