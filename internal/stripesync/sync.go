// Package stripesync reports usage to Stripe Billing meters.
//
// Stripe meter events are increments that Stripe sums. meterline therefore
// reports deltas: for each (customer, meter, period) it remembers the total
// already reported and sends only the difference. Each delta's identifier is
// derived from (customer, meter, period, new total), so a sync that crashes
// after Stripe accepted the event but before the new total was recorded sends
// the same identifier again and Stripe drops it as a duplicate.
package stripesync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/amaanmithani/meterline/internal/store"
	"github.com/stripe/stripe-go/v82"
)

// Sender sends one meter event to Stripe.
type Sender interface {
	Send(ctx context.Context, eventName, identifier, stripeCustomer string, value int64, ts time.Time) error
}

// Ledger remembers what has been reported.
type Ledger interface {
	Reported(ctx context.Context, customer, meter, period string) (int64, error)
	SetReported(ctx context.Context, customer, meter, period string, total int64) error
}

// Usage totals for one customer and period.
type Usage interface {
	Totals(ctx context.Context, customer string, from, to, cutoff time.Time) ([]store.Usage, error)
}

// Config maps meterline names to Stripe's.
type Config struct {
	StripeCustomers map[string]string // meterline customer -> Stripe customer id
	EventNames      map[string]string // meter -> Stripe meter event_name
}

// Syncer runs a sync.
type Syncer struct {
	cfg    Config
	usage  Usage
	ledger Ledger
	send   Sender
	now    func() time.Time
}

// New builds a Syncer.
func New(cfg Config, usage Usage, ledger Ledger, send Sender) *Syncer {
	return &Syncer{cfg: cfg, usage: usage, ledger: ledger, send: send, now: time.Now}
}

// Identifier is the idempotency key for reporting total for a series.
func Identifier(customer, meter, period string, total int64) string {
	h := sha256.Sum256([]byte(customer + "\x00" + meter + "\x00" + period + "\x00" + strconv.FormatInt(total, 10)))
	return "ml_" + hex.EncodeToString(h[:16])
}

// Result summarises a sync.
type Result struct {
	Sent     int
	Skipped  int // no change since the last report
	Unmapped int // customer or meter without a Stripe mapping
}

// Sync reports every mapped customer's usage for [from, to) as of now.
func (s *Syncer) Sync(ctx context.Context, period string, from, to time.Time) (Result, error) {
	var r Result
	for cust, stripeID := range s.cfg.StripeCustomers {
		totals, err := s.usage.Totals(ctx, cust, from, to, s.now())
		if err != nil {
			return r, err
		}
		for _, u := range totals {
			name, ok := s.cfg.EventNames[u.Meter]
			if !ok {
				r.Unmapped++
				continue
			}
			prev, err := s.ledger.Reported(ctx, cust, u.Meter, period)
			if err != nil {
				return r, err
			}
			delta := u.Value - prev
			if delta <= 0 {
				// Totals only grow (events are never deleted); <= 0 means
				// nothing new.
				r.Skipped++
				continue
			}
			id := Identifier(cust, u.Meter, period, u.Value)
			if err := s.send.Send(ctx, name, id, stripeID, delta, s.now()); err != nil {
				return r, fmt.Errorf("stripe %s/%s: %w", cust, u.Meter, err)
			}
			if err := s.ledger.SetReported(ctx, cust, u.Meter, period, u.Value); err != nil {
				return r, err
			}
			r.Sent++
		}
	}
	return r, nil
}

// StripeSender sends through the Stripe API (or stripe-mock).
type StripeSender struct {
	c *stripe.Client
}

// NewStripeSender uses key against baseURL ("" = api.stripe.com).
func NewStripeSender(key, baseURL string) *StripeSender {
	var opts []stripe.ClientOption
	if baseURL != "" {
		backends := stripe.NewBackends(nil)
		backends.API = stripe.GetBackendWithConfig(stripe.APIBackend, &stripe.BackendConfig{URL: stripe.String(baseURL)})
		opts = append(opts, stripe.WithBackends(backends))
	}
	return &StripeSender{c: stripe.NewClient(key, opts...)}
}

// Send implements Sender.
func (s *StripeSender) Send(ctx context.Context, eventName, identifier, stripeCustomer string, value int64, ts time.Time) error {
	_, err := s.c.V1BillingMeterEvents.Create(ctx, &stripe.BillingMeterEventCreateParams{EventName: stripe.String(eventName),
		Identifier: stripe.String(identifier),
		Payload:    map[string]string{"stripe_customer_id": stripeCustomer, "value": strconv.FormatInt(value, 10)},
		Timestamp:  stripe.Int64(ts.Unix())})
	return err
}
