// Package it holds integration tests against real Redpanda and ClickHouse.
// They run when MET_KAFKA and MET_CLICKHOUSE are set (docker compose up).
package it

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/amaanmithani/meterline/internal/event"
	"github.com/amaanmithani/meterline/internal/pipeline"
	"github.com/amaanmithani/meterline/internal/store"
)

func env(t *testing.T) (string, store.Config) {
	t.Helper()
	k, ch := os.Getenv("MET_KAFKA"), os.Getenv("MET_CLICKHOUSE")
	if k == "" || ch == "" {
		t.Skip("set MET_KAFKA and MET_CLICKHOUSE to run integration tests (docker compose up)")
	}
	return k, store.Config{Addr: ch, Database: "meterline", User: "meterline", Password: "meterline"}
}

func openStore(t *testing.T, cfg store.Config) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Truncate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func sum(u []store.Usage) map[string]int64 {
	m := map[string]int64{}
	for _, x := range u {
		m[x.Meter] = x.Value
	}
	return m
}

func TestStoreDedupAndLateEvents(t *testing.T) {
	_, cfg := env(t)
	s := openStore(t, cfg)
	ctx := context.Background()
	sept := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	evs := []event.Event{
		{ID: "a", Customer: "acme", Meter: "calls", Value: 5, TS: sept},
		{ID: "b", Customer: "acme", Meter: "calls", Value: 7, TS: sept},
	}
	if err := s.Insert(ctx, evs, "tok-1"); err != nil {
		t.Fatal(err)
	}
	// Same batch again with the same token: dropped by ClickHouse.
	_ = s.Insert(ctx, evs, "tok-1")
	// Producer duplicate in a different batch: stored, but counted once.
	_ = s.Insert(ctx, evs[:1], "tok-2")
	rows, _ := s.RawRows(ctx)
	from, to := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	u, err := s.Totals(ctx, "acme", from, to, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if sum(u)["calls"] != 12 {
		t.Fatalf("totals %v (raw rows %d)", u, rows)
	}
	if rows != 3 {
		t.Fatalf("token dedup: raw rows %d, want 3 (2 + 1 producer duplicate)", rows)
	}
	// Late: the cutoff excludes everything ingested after it.
	cut := time.Now().Add(-time.Hour)
	if u, _ := s.Totals(ctx, "acme", from, to, cut); len(u) != 0 {
		t.Fatalf("cutoff before ingestion should exclude all: %v", u)
	}
	late, _ := s.LateTotals(ctx, "acme", from, to, cut, time.Now().Add(time.Hour))
	if sum(late)["calls"] != 12 {
		t.Fatalf("late totals %v", late)
	}
	h, _ := s.HourlyUsage(ctx, "acme", from, to)
	if len(h) != 1 || h[0].Value != 12 {
		t.Fatalf("hourly %v", h)
	}
	cs, _ := s.Customers(ctx, from, to)
	found := false
	for _, c := range cs {
		found = found || c == "acme"
	}
	if !found {
		t.Fatalf("customers %v", cs)
	}
	if err := s.SetReported(ctx, "acme", "calls", "2026-09", 12); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Reported(ctx, "acme", "calls", "2026-09"); n != 12 {
		t.Fatalf("reported %d", n)
	}
}

// TestExactlyOnceUnderDuplicatesAndConsumerCrashes is the core claim:
// whatever duplicates producers send and wherever consumers crash, totals
// equal the sum over distinct event ids.
func TestExactlyOnceUnderDuplicatesAndConsumerCrashes(t *testing.T) {
	brokers, cfg := env(t)
	s := openStore(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	topic := fmt.Sprintf("usage-it-%d", time.Now().UnixNano())
	if err := pipeline.EnsureTopic(ctx, []string{brokers}, topic, 4); err != nil {
		t.Fatal(err)
	}
	p, err := pipeline.NewProducer([]string{brokers}, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	rng := rand.New(rand.NewSource(1))
	ts := time.Now().Add(-time.Minute).UTC()
	want := map[string]int64{}
	var sent []event.Event
	for i := 0; i < 5000; i++ {
		cust := fmt.Sprint("c", i%7)
		e := event.Event{ID: fmt.Sprint("e", i), Customer: cust, Meter: "tokens", Value: int64(1 + rng.Intn(100)), TS: ts}
		want[cust] += e.Value
		sent = append(sent, e)
		if rng.Intn(2) == 0 { // 50% of events sent twice (producer retries)
			sent = append(sent, e)
		}
	}
	rng.Shuffle(len(sent), func(i, j int) { sent[i], sent[j] = sent[j], sent[i] })
	for i := 0; i < len(sent); i += 500 {
		if err := p.Produce(ctx, sent[i:min(i+500, len(sent))]); err != nil {
			t.Fatal(err)
		}
	}

	// Consumer i commits i polls, then crashes after inserting poll i+1:
	// committed progress plus a redelivered, already-inserted batch.
	group := topic + "-g"
	crashes := 0
	for crashes < 4 {
		polls, crashAt := 0, crashes+1
		c, err := pipeline.NewConsumer(pipeline.ConsumerConfig{Brokers: []string{brokers}, Topic: topic, Group: group,
			MaxPollRecords: 700, FailAfterInsert: func() bool { polls++; return polls >= crashAt }}, s)
		if err != nil {
			t.Fatal(err)
		}
		cctx, ccancel := context.WithTimeout(ctx, 30*time.Second)
		err = c.Run(cctx)
		ccancel()
		c.Close()
		if !errors.Is(err, pipeline.ErrSimulatedCrash) {
			t.Fatalf("consumer %d: %v (inserted %d in %d batches before stopping)", crashes, err, c.Inserted.Load(), c.Batches.Load())
		}
		crashes++
	}
	// A final consumer drains the rest without crashing.
	c, err := pipeline.NewConsumer(pipeline.ConsumerConfig{Brokers: []string{brokers}, Topic: topic, Group: group,
		MaxPollRecords: 700}, s)
	if err != nil {
		t.Fatal(err)
	}
	dctx, dcancel := context.WithTimeout(ctx, 20*time.Second)
	done := make(chan error, 1)
	go func() { done <- c.Run(dctx) }()
	from, to := ts.Add(-time.Hour), ts.Add(time.Hour)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for cust, w := range want {
			u, err := s.Totals(ctx, cust, from, to, time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if sum(u)["tokens"] != w {
				ok = false
				break
			}
		}
		if ok {
			dcancel()
			<-done
			c.Close()
			raw, _ := s.RawRows(ctx)
			t.Logf("sent %d events (5000 distinct), %d consumer crashes, %d raw rows stored, totals exact",
				len(sent), crashes, raw)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	dcancel()
	<-done
	c.Close()
	for cust, w := range want {
		u, _ := s.Totals(ctx, cust, from, to, time.Now().Add(time.Hour))
		t.Errorf("%s: got %d want %d", cust, sum(u)["tokens"], w)
	}
}
