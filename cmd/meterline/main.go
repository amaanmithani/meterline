// Command meterline runs the ingest API and consumer (serve), a Stripe usage
// sync (stripe-sync), or schema migration (migrate).
//
//	meterline -config meterline.yaml serve
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/amaanmithani/meterline/internal/api"
	"github.com/amaanmithani/meterline/internal/billing"
	"github.com/amaanmithani/meterline/internal/pipeline"
	"github.com/amaanmithani/meterline/internal/store"
	"github.com/amaanmithani/meterline/internal/stripesync"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"
)

// Config is the YAML file.
type Config struct {
	Listen string `yaml:"listen"`
	Kafka  struct {
		Brokers    []string `yaml:"brokers"`
		Topic      string   `yaml:"topic"`
		Partitions int32    `yaml:"partitions"`
		Group      string   `yaml:"group"`
	} `yaml:"kafka"`
	ClickHouse    store.Config            `yaml:"clickhouse"`
	Plans         map[string]billing.Plan `yaml:"plans"`
	DefaultPlan   string                  `yaml:"default_plan"`
	CustomerPlans map[string]string       `yaml:"customer_plans"`
	Grace         time.Duration           `yaml:"grace"`
	IngestKeysEnv string                  `yaml:"ingest_keys_env"` // comma-separated keys in this env var
	ReadKeysEnv   string                  `yaml:"read_keys_env"`
	Stripe        struct {
		KeyEnv    string            `yaml:"key_env"`
		BaseURL   string            `yaml:"base_url"`
		Customers map[string]string `yaml:"customers"`
		Meters    map[string]string `yaml:"meters"`
	} `yaml:"stripe"`
}

func load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if c.Listen == "" {
		c.Listen = ":8090"
	}
	if c.Kafka.Topic == "" {
		c.Kafka.Topic = pipeline.Topic
	}
	if c.Kafka.Group == "" {
		c.Kafka.Group = "meterline"
	}
	if c.Kafka.Partitions == 0 {
		c.Kafka.Partitions = 8
	}
	if c.Grace == 0 {
		c.Grace = time.Hour
	}
	return &c, nil
}

func hashedKeys(env string) []string {
	var out []string
	for _, k := range strings.Split(os.Getenv(env), ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, api.HashKey(k))
		}
	}
	return out
}

func main() {
	path := flag.String("config", "meterline.yaml", "config file")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cmd := flag.Arg(0)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cmd, *path, logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd, path string, logger *slog.Logger) error {
	cfg, err := load(path)
	if err != nil {
		return err
	}
	st, err := store.Open(ctx, cfg.ClickHouse)
	if err != nil {
		return err
	}
	defer st.Close()
	switch cmd {
	case "migrate":
		logger.Info("schema up to date")
		return nil
	case "stripe-sync":
		return stripeSync(ctx, cfg, st, logger)
	case "serve":
		return serve(ctx, cfg, st, logger)
	default:
		return fmt.Errorf("usage: meterline -config FILE serve|stripe-sync|migrate")
	}
}

func serve(ctx context.Context, cfg *Config, st *store.Store, logger *slog.Logger) error {
	if err := pipeline.EnsureTopic(ctx, cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.Partitions); err != nil {
		return err
	}
	prod, err := pipeline.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	if err != nil {
		return err
	}
	defer prod.Close()
	reg := prometheus.NewRegistry()
	srv, err := api.New(api.Config{Producer: prod, Store: st, Plans: cfg.Plans, DefaultPlan: cfg.DefaultPlan,
		CustomerPlans: cfg.CustomerPlans, Grace: cfg.Grace, IngestKeySHA256: hashedKeys(cfg.IngestKeysEnv),
		ReadKeySHA256: hashedKeys(cfg.ReadKeysEnv), Logger: logger, Registry: reg})
	if err != nil {
		return err
	}
	cons, err := pipeline.NewConsumer(pipeline.ConsumerConfig{Brokers: cfg.Kafka.Brokers, Topic: cfg.Kafka.Topic,
		Group: cfg.Kafka.Group, MaxPollRecords: 20_000}, st)
	if err != nil {
		return err
	}
	defer cons.Close()
	pipeline.RegisterMetrics(reg, cons)
	errc := make(chan error, 2)
	go func() { errc <- cons.Run(ctx) }()
	hs := &http.Server{Addr: cfg.Listen, Handler: srv.Handler(dashboard()), ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second}
	go func() {
		logger.Info("meterline serving", "addr", cfg.Listen)
		if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	select {
	case <-ctx.Done():
	case err := <-errc:
		if err != nil {
			return err
		}
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return hs.Shutdown(sctx)
}

func stripeSync(ctx context.Context, cfg *Config, st *store.Store, logger *slog.Logger) error {
	key := os.Getenv(cfg.Stripe.KeyEnv)
	if key == "" {
		return fmt.Errorf("stripe key env %q is empty", cfg.Stripe.KeyEnv)
	}
	period := time.Now().UTC().Format("2006-01")
	from, to, _ := api.PeriodBounds(period)
	s := stripesync.New(stripesync.Config{StripeCustomers: cfg.Stripe.Customers, EventNames: cfg.Stripe.Meters}, st, st,
		stripesync.NewStripeSender(key, cfg.Stripe.BaseURL))
	res, err := s.Sync(ctx, period, from, to)
	logger.Info("stripe sync", "period", period, "sent", res.Sent, "skipped", res.Skipped, "unmapped", res.Unmapped)
	return err
}
