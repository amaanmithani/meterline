package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestServeEndToEnd runs the real binary's serve path against the compose
// stack: HTTP ingest -> Redpanda -> consumer -> ClickHouse -> invoice API.
func TestServeEndToEnd(t *testing.T) {
	k, ch := os.Getenv("MET_KAFKA"), os.Getenv("MET_CLICKHOUSE")
	if k == "" || ch == "" {
		t.Skip("set MET_KAFKA and MET_CLICKHOUSE (docker compose up)")
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	topic := fmt.Sprintf("usage-e2e-%d", time.Now().UnixNano())
	cfg := fmt.Sprintf(`
listen: %q
kafka: {brokers: [%q], topic: %q, partitions: 2, group: %q}
clickhouse: {addr: %q, database: meterline, user: meterline, password: meterline}
default_plan: pro
grace: 1h
ingest_keys_env: E2E_INGEST_KEYS
plans:
  pro:
    name: pro
    currency: usd
    meters:
      calls: {included: 10, price_per: 1, tiers: [{up_to: 0, price: "0.5"}]}
`, addr, k, topic, topic+"-g", ch)
	path := filepath.Join(t.TempDir(), "c.yaml")
	_ = os.WriteFile(path, []byte(cfg), 0o600)
	t.Setenv("E2E_INGEST_KEYS", "k1, k2")
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- run(ctx, "serve", path, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	defer func() {
		cancel()
		if err := <-errc; err != nil {
			t.Error(err)
		}
	}()
	base := "http://" + addr
	for i := 0; i < 100; i++ {
		if r, err := http.Get(base + "/healthz"); err == nil {
			r.Body.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cust := fmt.Sprint("e2e", time.Now().UnixNano())
	ts := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	var events []string
	for i := 0; i < 30; i++ {
		events = append(events, fmt.Sprintf(`{"id":"%s-%d","customer":%q,"meter":"calls","value":1,"ts":%q}`, cust, i, cust, ts))
	}
	body := "[" + strings.Join(events, ",") + "]"
	for i := 0; i < 2; i++ { // send twice: the second batch is all duplicates
		req, _ := http.NewRequest("POST", base+"/v1/events", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer k2")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 202 {
			t.Fatalf("ingest: %v %v", err, resp)
		}
		resp.Body.Close()
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, err := http.Get(base + "/v1/customers/" + cust + "/invoice")
		if err == nil {
			var inv struct {
				Lines []struct {
					Quantity int64 `json:"quantity"`
				} `json:"lines"`
				Total string `json:"total"`
			}
			_ = json.NewDecoder(r.Body).Decode(&inv)
			r.Body.Close()
			if len(inv.Lines) == 1 && inv.Lines[0].Quantity == 30 {
				if inv.Total != "10" { // 20 billable at 0.5
					t.Fatalf("total %s", inv.Total)
				}
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("events never showed up exactly once on the invoice")
}

func TestRunErrors(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run(context.Background(), "serve", "/nope.yaml", logger); err == nil {
		t.Fatal("missing config")
	}
	path := filepath.Join(t.TempDir(), "c.yaml")
	_ = os.WriteFile(path, []byte("bogus_field: 1"), 0o600)
	if err := run(context.Background(), "serve", path, logger); err == nil {
		t.Fatal("unknown field")
	}
}

func TestMigrateAndStripeSync(t *testing.T) {
	k, ch, st := os.Getenv("MET_KAFKA"), os.Getenv("MET_CLICKHOUSE"), os.Getenv("MET_STRIPE")
	if k == "" || ch == "" || st == "" {
		t.Skip("set MET_KAFKA, MET_CLICKHOUSE and MET_STRIPE")
	}
	cfg := fmt.Sprintf(`
kafka: {brokers: [%q]}
clickhouse: {addr: %q, database: meterline, user: meterline, password: meterline}
default_plan: pro
plans: {pro: {name: pro, currency: usd, meters: {calls: {tiers: [{up_to: 0, price: "1"}]}}}}
stripe:
  key_env: E2E_STRIPE_KEY
  base_url: %q
  customers: {acme: cus_123}
  meters: {calls: api_calls}
`, k, ch, st)
	path := filepath.Join(t.TempDir(), "c.yaml")
	_ = os.WriteFile(path, []byte(cfg), 0o600)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run(context.Background(), "migrate", path, logger); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), "stripe-sync", path, logger); err == nil {
		t.Fatal("stripe-sync without a key should fail")
	}
	t.Setenv("E2E_STRIPE_KEY", "sk_test_123")
	if err := run(context.Background(), "stripe-sync", path, logger); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), "bogus", path, logger); err == nil {
		t.Fatal("unknown command")
	}
}
