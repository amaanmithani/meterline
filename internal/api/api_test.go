package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/amaanmithani/meterline/internal/billing"
	"github.com/amaanmithani/meterline/internal/event"
	"github.com/amaanmithani/meterline/internal/store"
	"github.com/shopspring/decimal"
)

type fakeProducer struct {
	got  []event.Event
	fail bool
}

func (f *fakeProducer) Produce(_ context.Context, evs []event.Event) error {
	if f.fail {
		return errors.New("broker down")
	}
	f.got = append(f.got, evs...)
	return nil
}

// fakeStore answers from an in-memory event list with the same semantics as
// the ClickHouse store: dedup by id, earliest ingestion wins.
type fakeStore struct {
	rows []struct {
		e   event.Event
		ing time.Time
	}
}

func (f *fakeStore) add(e event.Event, ing time.Time) {
	f.rows = append(f.rows, struct {
		e   event.Event
		ing time.Time
	}{e, ing})
}

func (f *fakeStore) dedup(customer string, from, to time.Time) map[string]struct {
	e   event.Event
	ing time.Time
} {
	out := map[string]struct {
		e   event.Event
		ing time.Time
	}{}
	for _, r := range f.rows {
		if r.e.Customer != customer || r.e.TS.Before(from) || !r.e.TS.Before(to) {
			continue
		}
		if cur, ok := out[r.e.ID]; !ok || r.ing.Before(cur.ing) {
			out[r.e.ID] = r
		}
	}
	return out
}

func (f *fakeStore) Totals(_ context.Context, c string, from, to, cutoff time.Time) ([]store.Usage, error) {
	m := map[string]int64{}
	for _, r := range f.dedup(c, from, to) {
		if !r.ing.After(cutoff) {
			m[r.e.Meter] += r.e.Value
		}
	}
	return toUsage(m), nil
}

func (f *fakeStore) LateTotals(_ context.Context, c string, from, to, after, until time.Time) ([]store.Usage, error) {
	m := map[string]int64{}
	for _, r := range f.dedup(c, from, to) {
		if r.ing.After(after) && !r.ing.After(until) {
			m[r.e.Meter] += r.e.Value
		}
	}
	return toUsage(m), nil
}

func toUsage(m map[string]int64) []store.Usage {
	var out []store.Usage
	for k, v := range m {
		out = append(out, store.Usage{Meter: k, Value: v})
	}
	return out
}

func (f *fakeStore) HourlyUsage(_ context.Context, c string, from, to time.Time) ([]store.Hourly, error) {
	return []store.Hourly{{Hour: from.Truncate(time.Hour), Meter: "m", Value: 1}}, nil
}

func (f *fakeStore) Customers(context.Context, time.Time, time.Time) ([]string, error) {
	return []string{"acme"}, nil
}

var plan = billing.Plan{Name: "pro", Currency: "usd", Meters: map[string]billing.MeterPrice{
	"calls": {PricePer: 1, Tiers: []billing.Tier{{Price: decimal.RequireFromString("0.01")}}},
}}

func newServer(t *testing.T, now time.Time) (*Server, *fakeProducer, *fakeStore) {
	t.Helper()
	p, st := &fakeProducer{}, &fakeStore{}
	s, err := New(Config{Producer: p, Store: st, Plans: map[string]billing.Plan{"pro": plan}, DefaultPlan: "pro",
		Grace: time.Hour, IngestKeySHA256: []string{HashKey("ingest-key")}, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return s, p, st
}

func do(h http.Handler, method, path, key, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestIngest(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	s, p, _ := newServer(t, now)
	h := s.Handler(nil)
	good := `[{"id":"a","customer":"acme","meter":"calls","value":2,"ts":"2026-09-24T11:00:00Z"}]`
	if w := do(h, "POST", "/v1/events", "", good); w.Code != 401 {
		t.Fatalf("no key: %d", w.Code)
	}
	if w := do(h, "POST", "/v1/events", "wrong", good); w.Code != 401 {
		t.Fatalf("wrong key: %d", w.Code)
	}
	if w := do(h, "POST", "/v1/events", "ingest-key", good); w.Code != 202 || len(p.got) != 1 {
		t.Fatalf("good: %d %s", w.Code, w.Body)
	}
	if w := do(h, "POST", "/v1/events", "ingest-key", `nope`); w.Code != 400 {
		t.Fatalf("bad json: %d", w.Code)
	}
	mixed := `[{"id":"b","customer":"acme","meter":"calls","value":1,"ts":"2026-09-24T11:00:00Z"},
	           {"id":"c","customer":"acme","meter":"calls","value":-1,"ts":"2026-09-24T11:00:00Z"}]`
	if w := do(h, "POST", "/v1/events", "ingest-key", mixed); w.Code != 422 || len(p.got) != 1 {
		t.Fatalf("one invalid event must reject the whole batch: %d, produced %d", w.Code, len(p.got))
	}
	p.fail = true
	if w := do(h, "POST", "/v1/events", "ingest-key", good); w.Code != 503 {
		t.Fatalf("producer failure must not be acknowledged: %d", w.Code)
	}
	if w := do(h, "POST", "/v1/events", "ingest-key", strings.Repeat("x", 9<<20)); w.Code != 413 {
		t.Fatalf("oversized: %d", w.Code)
	}
}

func TestInvoiceWithLateUsage(t *testing.T) {
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	s, _, st := newServer(t, now)
	sep := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	oct := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	st.add(event.Event{ID: "1", Customer: "acme", Meter: "calls", Value: 100, TS: sep}, sep)                                          // on time
	st.add(event.Event{ID: "1", Customer: "acme", Meter: "calls", Value: 100, TS: sep}, oct)                                          // duplicate, later
	st.add(event.Event{ID: "2", Customer: "acme", Meter: "calls", Value: 50, TS: sep}, time.Date(2026, 10, 1, 0, 30, 0, 0, time.UTC)) // within grace
	st.add(event.Event{ID: "3", Customer: "acme", Meter: "calls", Value: 7, TS: sep}, oct)                                            // late for September
	st.add(event.Event{ID: "4", Customer: "acme", Meter: "calls", Value: 20, TS: oct}, oct)                                           // October
	h := s.Handler(nil)
	var sepInv, octInv billing.Invoice
	_ = json.Unmarshal(do(h, "GET", "/v1/customers/acme/invoice?period=2026-09", "", "").Body.Bytes(), &sepInv)
	_ = json.Unmarshal(do(h, "GET", "/v1/customers/acme/invoice?period=2026-10", "", "").Body.Bytes(), &octInv)
	if len(sepInv.Lines) != 1 || sepInv.Lines[0].Quantity != 150 || !sepInv.Final {
		t.Fatalf("september (final, dedup, grace): %+v", sepInv)
	}
	if octInv.Lines[0].Quantity != 20 || len(octInv.Adjustments) != 1 || octInv.Adjustments[0].Quantity != 7 ||
		octInv.Final || !octInv.Total.Equal(decimal.RequireFromString("0.27")) {
		t.Fatalf("october (late adjustment, open): %+v", octInv)
	}
	if w := do(h, "GET", "/v1/customers/acme/invoice?period=bad", "", ""); w.Code != 400 {
		t.Fatalf("bad period: %d", w.Code)
	}
	if w := do(h, "GET", "/v1/customers/acme/invoice", "", ""); w.Code != 200 {
		t.Fatalf("default period: %d", w.Code)
	}
}

func TestReadsGuardedAndUsage(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	p := &fakeProducer{}
	s, err := New(Config{Producer: p, Store: &fakeStore{}, Plans: map[string]billing.Plan{"pro": plan}, DefaultPlan: "pro",
		ReadKeySHA256: []string{HashKey("reader")}, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler(http.NotFoundHandler())
	if w := do(h, "GET", "/v1/customers", "", ""); w.Code != 401 {
		t.Fatalf("unguarded read: %d", w.Code)
	}
	if w := do(h, "GET", "/v1/customers", "reader", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "acme") {
		t.Fatalf("customers: %d %s", w.Code, w.Body)
	}
	if w := do(h, "GET", "/v1/customers/acme/usage", "reader", ""); w.Code != 200 {
		t.Fatalf("usage: %d", w.Code)
	}
	for _, q := range []string{"from=bad", "to=bad", "from=2026-01-01T00:00:00Z&to=2026-09-01T00:00:00Z", "from=2026-09-02T00:00:00Z&to=2026-09-01T00:00:00Z"} {
		if w := do(h, "GET", "/v1/customers/acme/usage?"+q, "reader", ""); w.Code != 400 {
			t.Fatalf("%s: %d", q, w.Code)
		}
	}
	if w := do(h, "GET", "/v1/customers?period=13-2026", "reader", ""); w.Code != 400 {
		t.Fatalf("bad period: %d", w.Code)
	}
	for _, path := range []string{"/healthz", "/metrics"} {
		if w := do(h, "GET", path, "", ""); w.Code != 200 {
			t.Fatalf("%s: %d", path, w.Code)
		}
	}
}

func TestNewValidatesPlans(t *testing.T) {
	base := Config{Producer: &fakeProducer{}, Store: &fakeStore{}, Plans: map[string]billing.Plan{"pro": plan}}
	c := base
	c.DefaultPlan = "missing"
	if _, err := New(c); err == nil {
		t.Fatal("missing default plan")
	}
	c = base
	c.DefaultPlan, c.CustomerPlans = "pro", map[string]string{"x": "nope"}
	if _, err := New(c); err == nil {
		t.Fatal("unknown customer plan")
	}
	c = base
	c.DefaultPlan, c.Plans = "bad", map[string]billing.Plan{"bad": {Meters: map[string]billing.MeterPrice{"m": {}}}}
	if _, err := New(c); err == nil {
		t.Fatal("invalid plan")
	}
}
