// Package store keeps usage events in ClickHouse and answers the queries
// billing and dashboards need.
//
// Exactly-once counting rests on one rule: every query that feeds money
// deduplicates by event id (GROUP BY id), keeping the earliest ingestion. That
// holds no matter how many times an event was inserted: producer retries,
// consumer redelivery after a crash, or a redelivered batch split at different
// offsets. The insert dedup token only reduces duplicate rows on disk.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/amaanmithani/meterline/internal/event"
)

// Store is a ClickHouse-backed event store.
type Store struct {
	conn driver.Conn
	now  func() time.Time
}

// Config configures the connection.
type Config struct {
	Addr     string // host:port of the native protocol
	Database string
	User     string
	Password string
}

// Open connects and migrates.
func Open(ctx context.Context, c Config) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{c.Addr},
		Auth: clickhouse.Auth{Database: c.Database, Username: c.User, Password: c.Password},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	s := &Store{conn: conn, now: time.Now}
	return s, s.migrate(ctx)
}

// Close closes the connection.
func (s *Store) Close() error { return s.conn.Close() }

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		// ver = 2^62 - ingest millis: ReplacingMergeTree keeps the highest
		// version, i.e. the EARLIEST ingestion, so a late duplicate can't turn
		// an on-time event into a late one when parts merge.
		`CREATE TABLE IF NOT EXISTS events (
			id String,
			customer LowCardinality(String),
			meter LowCardinality(String),
			value Int64,
			ts DateTime64(3, 'UTC'),
			ingested_at DateTime64(3, 'UTC'),
			ver UInt64
		) ENGINE = ReplacingMergeTree(ver)
		PARTITION BY toYYYYMM(ts)
		ORDER BY (customer, meter, id)
		SETTINGS non_replicated_deduplication_window = 10000`,
		`CREATE TABLE IF NOT EXISTS stripe_reports (
			customer LowCardinality(String),
			meter LowCardinality(String),
			period String,
			reported Int64,
			updated_at DateTime64(3, 'UTC')
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (customer, meter, period)`,
	}
	for _, q := range stmts {
		if err := s.conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

func version(ingested time.Time) uint64 { return uint64(1<<62) - uint64(ingested.UnixMilli()) }

// Insert writes a batch. token, if non-empty, makes a retried insert of the
// same batch a no-op in ClickHouse (insert_deduplication_token).
func (s *Store) Insert(ctx context.Context, events []event.Event, token string) error {
	if len(events) == 0 {
		return nil
	}
	if token != "" {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"insert_deduplication_token": token}))
	}
	b, err := s.conn.PrepareBatch(ctx, "INSERT INTO events (id, customer, meter, value, ts, ingested_at, ver)")
	if err != nil {
		return err
	}
	now := s.now().UTC()
	for _, e := range events {
		if err := b.Append(e.ID, e.Customer, e.Meter, e.Value, e.TS.UTC(), now, version(now)); err != nil {
			_ = b.Abort()
			return err
		}
	}
	return b.Send()
}

// Usage is a meter total.
type Usage struct {
	Meter string
	Value int64
}

// dedup is the canonical deduplicated view: one row per event id with the
// earliest ingestion. Every billing query builds on it.
const dedup = `
	SELECT id, any(meter) AS m, any(value) AS v, min(ts) AS ets, min(ingested_at) AS ing
	FROM events
	WHERE customer = @customer AND ts >= @from AND ts < @to
	GROUP BY id`

// Totals returns per-meter totals for events with ts in [from, to) that were
// ingested at or before cutoff.
func (s *Store) Totals(ctx context.Context, customer string, from, to, cutoff time.Time) ([]Usage, error) {
	q := `SELECT m, sum(v) FROM (` + dedup + `) WHERE ing <= @cutoff GROUP BY m ORDER BY m`
	return s.usage(ctx, q, customer, from, to, cutoff, time.Time{})
}

// LateTotals returns per-meter totals for events with ts in [from, to) that
// were ingested in (after, until]: usage that arrived after its period closed.
func (s *Store) LateTotals(ctx context.Context, customer string, from, to, after, until time.Time) ([]Usage, error) {
	q := `SELECT m, sum(v) FROM (` + dedup + `) WHERE ing > @after AND ing <= @until GROUP BY m ORDER BY m`
	return s.usage(ctx, q, customer, from, to, after, until)
}

func (s *Store) usage(ctx context.Context, q, customer string, from, to, a, b time.Time) ([]Usage, error) {
	args := []any{clickhouse.Named("customer", customer), clickhouse.Named("from", from.UTC()), clickhouse.Named("to", to.UTC())}
	if !a.IsZero() {
		args = append(args, clickhouse.Named("cutoff", a.UTC()), clickhouse.Named("after", a.UTC()))
	}
	if !b.IsZero() {
		args = append(args, clickhouse.Named("until", b.UTC()))
	}
	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Usage
	for rows.Next() {
		var u Usage
		if err := rows.Scan(&u.Meter, &u.Value); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// Hourly is one dashboard point.
type Hourly struct {
	Hour  time.Time
	Meter string
	Value int64
}

// HourlyUsage returns deduplicated hourly totals for a customer.
func (s *Store) HourlyUsage(ctx context.Context, customer string, from, to time.Time) ([]Hourly, error) {
	q := `SELECT toStartOfHour(ets) AS h, m, sum(v) FROM (` + dedup + `) GROUP BY h, m ORDER BY h, m`
	rows, err := s.conn.Query(ctx, q, clickhouse.Named("customer", customer), clickhouse.Named("from", from.UTC()),
		clickhouse.Named("to", to.UTC()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Hourly
	for rows.Next() {
		var h Hourly
		if err := rows.Scan(&h.Hour, &h.Meter, &h.Value); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// Customers lists customers with events in [from, to).
func (s *Store) Customers(ctx context.Context, from, to time.Time) ([]string, error) {
	rows, err := s.conn.Query(ctx, `SELECT DISTINCT customer FROM events WHERE ts >= @from AND ts < @to ORDER BY customer`,
		clickhouse.Named("from", from.UTC()), clickhouse.Named("to", to.UTC()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// RawRows counts stored rows (duplicates included), for dedup metrics/tests.
func (s *Store) RawRows(ctx context.Context) (uint64, error) {
	var n uint64
	err := s.conn.QueryRow(ctx, `SELECT count() FROM events`).Scan(&n)
	return n, err
}

// Reported returns what was last reported to Stripe for (customer, meter, period).
func (s *Store) Reported(ctx context.Context, customer, meter, period string) (int64, error) {
	var n int64
	err := s.conn.QueryRow(ctx, `SELECT argMax(reported, updated_at) FROM stripe_reports
		WHERE customer = @c AND meter = @m AND period = @p`,
		clickhouse.Named("c", customer), clickhouse.Named("m", meter), clickhouse.Named("p", period)).Scan(&n)
	return n, err
}

// SetReported records a new reported total.
func (s *Store) SetReported(ctx context.Context, customer, meter, period string, total int64) error {
	return s.conn.Exec(ctx, `INSERT INTO stripe_reports (customer, meter, period, reported, updated_at) VALUES (?, ?, ?, ?, ?)`,
		customer, meter, period, total, s.now().UTC())
}

// Truncate empties all tables (tests and benchmarks).
func (s *Store) Truncate(ctx context.Context) error {
	for _, t := range []string{"events", "stripe_reports"} {
		if err := s.conn.Exec(ctx, "TRUNCATE TABLE "+t); err != nil {
			return err
		}
	}
	return nil
}
