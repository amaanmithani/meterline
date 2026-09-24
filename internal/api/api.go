// Package api is meterline's HTTP interface: ingest, invoices and usage.
package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/amaanmithani/meterline/internal/billing"
	"github.com/amaanmithani/meterline/internal/event"
	"github.com/amaanmithani/meterline/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Producer publishes accepted events durably.
type Producer interface {
	Produce(ctx context.Context, events []event.Event) error
}

// Store answers usage queries.
type Store interface {
	Totals(ctx context.Context, customer string, from, to, cutoff time.Time) ([]store.Usage, error)
	LateTotals(ctx context.Context, customer string, from, to, after, until time.Time) ([]store.Usage, error)
	HourlyUsage(ctx context.Context, customer string, from, to time.Time) ([]store.Hourly, error)
	Customers(ctx context.Context, from, to time.Time) ([]string, error)
}

// Config wires the API.
type Config struct {
	Producer Producer
	Store    Store
	Plans    map[string]billing.Plan // by name
	// CustomerPlans maps a customer to a plan name; others get DefaultPlan.
	CustomerPlans map[string]string
	DefaultPlan   string
	// Grace is how long after a period ends events for it are still on
	// time. Later ones become adjustments on the next invoice.
	Grace time.Duration
	// IngestKeySHA256 are hex SHA-256 hashes of accepted ingest keys.
	IngestKeySHA256 []string
	// ReadKeySHA256 guards invoice/usage reads (empty = public reads).
	ReadKeySHA256 []string
	Logger        *slog.Logger
	Registry      *prometheus.Registry
	Now           func() time.Time
}

// Server serves the API.
type Server struct {
	cfg      Config
	accepted prometheus.Counter
	rejected *prometheus.CounterVec
}

// New builds a server.
func New(c Config) (*Server, error) {
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if c.Registry == nil {
		c.Registry = prometheus.NewRegistry()
	}
	if _, ok := c.Plans[c.DefaultPlan]; !ok {
		return nil, fmt.Errorf("default plan %q not defined", c.DefaultPlan)
	}
	for name, p := range c.Plans {
		if err := p.Validate(); err != nil {
			return nil, fmt.Errorf("plan %s: %w", name, err)
		}
	}
	for cust, plan := range c.CustomerPlans {
		if _, ok := c.Plans[plan]; !ok {
			return nil, fmt.Errorf("customer %s: unknown plan %q", cust, plan)
		}
	}
	s := &Server{cfg: c,
		accepted: prometheus.NewCounter(prometheus.CounterOpts{Name: "meterline_events_accepted_total", Help: "Events accepted by ingest."}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "meterline_batches_rejected_total", Help: "Rejected ingest batches."}, []string{"reason"}),
	}
	c.Registry.MustRegister(s.accepted, s.rejected)
	return s, nil
}

// HashKey returns the hex SHA-256 of a key.
func HashKey(k string) string {
	h := sha256.Sum256([]byte(k))
	return hex.EncodeToString(h[:])
}

func authorized(r *http.Request, hashes []string) bool {
	key := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if key == "" {
		return false
	}
	got := HashKey(key)
	ok := false
	for _, h := range hashes {
		if subtle.ConstantTimeCompare([]byte(got), []byte(strings.ToLower(h))) == 1 {
			ok = true
		}
	}
	return ok
}

// Handler returns the HTTP handler. static serves the dashboard (may be nil).
func (s *Server) Handler(static http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", s.ingest)
	mux.HandleFunc("GET /v1/customers", s.read(s.customers))
	mux.HandleFunc("GET /v1/customers/{id}/invoice", s.read(s.invoice))
	mux.HandleFunc("GET /v1/customers/{id}/usage", s.read(s.usage))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.cfg.Registry, promhttp.HandlerOpts{}))
	if static != nil {
		mux.Handle("GET /", static)
	}
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	if !authorized(r, s.cfg.IngestKeySHA256) {
		s.rejected.WithLabelValues("auth").Inc()
		writeErr(w, http.StatusUnauthorized, "a valid ingest key is required")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8<<20))
	if err != nil {
		s.rejected.WithLabelValues("size").Inc()
		writeErr(w, http.StatusRequestEntityTooLarge, "batch larger than 8 MB")
		return
	}
	evs, err := event.Parse(body)
	if err != nil {
		s.rejected.WithLabelValues("parse").Inc()
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	now := s.cfg.Now()
	for i, e := range evs {
		if err := e.Validate(now); err != nil {
			// Reject the whole batch: partial acceptance makes producer
			// retries ambiguous.
			s.rejected.WithLabelValues("invalid").Inc()
			writeErr(w, http.StatusUnprocessableEntity, fmt.Sprintf("event %d (%s): %v", i, e.ID, err))
			return
		}
	}
	if err := s.cfg.Producer.Produce(r.Context(), evs); err != nil {
		s.rejected.WithLabelValues("produce").Inc()
		s.cfg.Logger.Error("produce failed", "err", err)
		writeErr(w, http.StatusServiceUnavailable, "could not durably accept the batch; retry it (events are idempotent by id)")
		return
	}
	s.accepted.Add(float64(len(evs)))
	writeJSON(w, http.StatusAccepted, map[string]int{"accepted": len(evs)})
}

func (s *Server) read(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(s.cfg.ReadKeySHA256) > 0 && !authorized(r, s.cfg.ReadKeySHA256) {
			writeErr(w, http.StatusUnauthorized, "a valid read key is required")
			return
		}
		h(w, r)
	}
}

// PeriodBounds returns [start, end) for "YYYY-MM" in UTC.
func PeriodBounds(period string) (time.Time, time.Time, error) {
	start, err := time.Parse("2006-01", period)
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("period must be YYYY-MM")
	}
	return start, start.AddDate(0, 1, 0), nil
}

func (s *Server) planFor(customer string) billing.Plan {
	if name, ok := s.cfg.CustomerPlans[customer]; ok {
		return s.cfg.Plans[name]
	}
	return s.cfg.Plans[s.cfg.DefaultPlan]
}

func toBilling(u []store.Usage) []billing.Usage {
	out := make([]billing.Usage, len(u))
	for i, x := range u {
		out[i] = billing.Usage{Meter: x.Meter, Quantity: x.Value}
	}
	return out
}

// Invoice computes the invoice for a customer and period.
func (s *Server) Invoice(ctx context.Context, customer, period string) (billing.Invoice, error) {
	start, end, err := PeriodBounds(period)
	if err != nil {
		return billing.Invoice{}, err
	}
	now := s.cfg.Now().UTC()
	closeAt := end.Add(s.cfg.Grace)
	cutoff := minTime(now, closeAt)
	onTime, err := s.cfg.Store.Totals(ctx, customer, start, end, cutoff)
	if err != nil {
		return billing.Invoice{}, err
	}
	prevStart := start.AddDate(0, -1, 0)
	prevClose := start.Add(s.cfg.Grace)
	late, err := s.cfg.Store.LateTotals(ctx, customer, prevStart, start, prevClose, cutoff)
	if err != nil {
		return billing.Invoice{}, err
	}
	prevOnTime, err := s.cfg.Store.Totals(ctx, customer, prevStart, start, prevClose)
	if err != nil {
		return billing.Invoice{}, err
	}
	return billing.Build(customer, period, s.planFor(customer), toBilling(onTime), toBilling(late), toBilling(prevOnTime),
		now.After(closeAt)), nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (s *Server) invoice(w http.ResponseWriter, r *http.Request) {
	period := r.URL.Query().Get("period")
	if period == "" {
		period = s.cfg.Now().UTC().Format("2006-01")
	}
	inv, err := s.Invoice(r.Context(), r.PathValue("id"), period)
	if err != nil {
		if strings.Contains(err.Error(), "YYYY-MM") {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		s.cfg.Logger.Error("invoice", "err", err)
		writeErr(w, http.StatusInternalServerError, "could not compute the invoice")
		return
	}
	writeJSON(w, http.StatusOK, inv)
}

func (s *Server) usage(w http.ResponseWriter, r *http.Request) {
	now := s.cfg.Now().UTC()
	from, to := now.Add(-48*time.Hour), now.Add(time.Minute)
	var err error
	if v := r.URL.Query().Get("from"); v != "" {
		if from, err = time.Parse(time.RFC3339, v); err != nil {
			writeErr(w, http.StatusBadRequest, "from must be RFC 3339")
			return
		}
	}
	if v := r.URL.Query().Get("to"); v != "" {
		if to, err = time.Parse(time.RFC3339, v); err != nil {
			writeErr(w, http.StatusBadRequest, "to must be RFC 3339")
			return
		}
	}
	if to.Sub(from) > 93*24*time.Hour || !to.After(from) {
		writeErr(w, http.StatusBadRequest, "range must be positive and at most 93 days")
		return
	}
	pts, err := s.cfg.Store.HourlyUsage(r.Context(), r.PathValue("id"), from, to)
	if err != nil {
		s.cfg.Logger.Error("usage", "err", err)
		writeErr(w, http.StatusInternalServerError, "could not read usage")
		return
	}
	type point struct {
		Hour  time.Time `json:"hour"`
		Meter string    `json:"meter"`
		Value int64     `json:"value"`
	}
	out := make([]point, len(pts))
	for i, p := range pts {
		out[i] = point{p.Hour, p.Meter, p.Value}
	}
	writeJSON(w, http.StatusOK, map[string]any{"customer": r.PathValue("id"), "from": from, "to": to, "points": out})
}

func (s *Server) customers(w http.ResponseWriter, r *http.Request) {
	period := r.URL.Query().Get("period")
	if period == "" {
		period = s.cfg.Now().UTC().Format("2006-01")
	}
	start, end, err := PeriodBounds(period)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	cs, err := s.cfg.Store.Customers(r.Context(), start.AddDate(0, -1, 0), end)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not list customers")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"period": period, "customers": cs})
}
