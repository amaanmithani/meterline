package stripesync

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/amaanmithani/meterline/internal/store"
)

type sent struct {
	name, id, cust string
	value          int64
}

type fakeSender struct {
	calls []sent
	fail  bool
}

func (f *fakeSender) Send(_ context.Context, name, id, cust string, v int64, _ time.Time) error {
	if f.fail {
		return errors.New("stripe down")
	}
	f.calls = append(f.calls, sent{name, id, cust, v})
	return nil
}

type fakeData struct {
	totals   map[string][]store.Usage
	reported map[string]int64
	failSet  bool
}

func (f *fakeData) Totals(_ context.Context, c string, _, _, _ time.Time) ([]store.Usage, error) {
	return f.totals[c], nil
}
func (f *fakeData) Reported(_ context.Context, c, m, p string) (int64, error) {
	return f.reported[c+m+p], nil
}
func (f *fakeData) SetReported(_ context.Context, c, m, p string, v int64) error {
	if f.failSet {
		return errors.New("ledger write failed")
	}
	f.reported[c+m+p] = v
	return nil
}

func TestSyncReportsDeltasIdempotently(t *testing.T) {
	d := &fakeData{totals: map[string][]store.Usage{"acme": {{Meter: "tok", Value: 100}, {Meter: "unmapped", Value: 5}}},
		reported: map[string]int64{}}
	snd := &fakeSender{}
	s := New(Config{StripeCustomers: map[string]string{"acme": "cus_1"}, EventNames: map[string]string{"tok": "tokens"}}, d, d, snd)
	ctx := context.Background()
	r, err := s.Sync(ctx, "2026-09", time.Time{}, time.Time{})
	if err != nil || r.Sent != 1 || r.Unmapped != 1 || snd.calls[0].value != 100 || snd.calls[0].cust != "cus_1" {
		t.Fatalf("first sync: %+v %v %+v", r, err, snd.calls)
	}
	// Nothing new: nothing sent.
	if r, _ := s.Sync(ctx, "2026-09", time.Time{}, time.Time{}); r.Sent != 0 || r.Skipped != 1 {
		t.Fatalf("second sync: %+v", r)
	}
	// Usage grows: only the delta goes out.
	d.totals["acme"][0].Value = 130
	_, _ = s.Sync(ctx, "2026-09", time.Time{}, time.Time{})
	if last := snd.calls[len(snd.calls)-1]; last.value != 30 {
		t.Fatalf("delta: %+v", last)
	}
	// Crash after Stripe accepted but before the ledger recorded it: the retry
	// carries the same identifier, so Stripe drops the duplicate.
	d.totals["acme"][0].Value = 150
	d.failSet = true
	if _, err := s.Sync(ctx, "2026-09", time.Time{}, time.Time{}); err == nil {
		t.Fatal("ledger failure should surface")
	}
	d.failSet = false
	_, _ = s.Sync(ctx, "2026-09", time.Time{}, time.Time{})
	n := len(snd.calls)
	if snd.calls[n-1].id != snd.calls[n-2].id || snd.calls[n-1].value != 20 {
		t.Fatalf("retry after a crash must reuse the identifier: %+v", snd.calls[n-2:])
	}
	if Identifier("a", "m", "p", 1) == Identifier("a", "m", "p", 2) || Identifier("a", "m", "p", 1) == Identifier("b", "m", "p", 1) {
		t.Fatal("identifiers must differ per series and total")
	}
	snd.fail = true
	d.totals["acme"][0].Value = 999
	if _, err := s.Sync(ctx, "2026-09", time.Time{}, time.Time{}); err == nil {
		t.Fatal("stripe failure should surface")
	}
}

func TestStripeMock(t *testing.T) {
	url := os.Getenv("MET_STRIPE")
	if url == "" {
		t.Skip("set MET_STRIPE (e.g. http://localhost:12111) to test against stripe-mock")
	}
	s := NewStripeSender("sk_test_123", url)
	if err := s.Send(context.Background(), "tokens", "ml_test", "cus_123", 42, time.Now()); err != nil {
		t.Fatal(err)
	}
}
