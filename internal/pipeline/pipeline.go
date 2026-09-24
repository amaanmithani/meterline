// Package pipeline moves events through Redpanda: a producer for the ingest
// API and a consumer that writes batches to the store.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/amaanmithani/meterline/internal/event"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic is the default Kafka topic events flow through.
const Topic = "usage"

// EnsureTopic creates the topic if needed.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int32) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	res, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
	if err != nil {
		return err
	}
	for _, r := range res {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			return r.Err
		}
	}
	return nil
}

// Producer publishes events keyed by customer, so one customer's events stay
// ordered within a partition. Produce returns only after every record is
// acknowledged by the broker (acks=all), so the ingest API can answer 202
// only for events that are durable in the log.
type Producer struct {
	cl *kgo.Client
}

// NewProducer connects a producer. franz-go's idempotent producer is on by
// default: broker-side dedup of retried sends within a session.
func NewProducer(brokers []string, topic string) (*Producer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerLinger(2*time.Millisecond),
		kgo.ProducerBatchMaxBytes(1<<20))
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl}, nil
}

// Produce publishes and waits for acknowledgement of every event.
func (p *Producer) Produce(ctx context.Context, events []event.Event) error {
	recs := make([]*kgo.Record, len(events))
	for i, e := range events {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		recs[i] = &kgo.Record{Key: []byte(e.Customer), Value: b}
	}
	return p.cl.ProduceSync(ctx, recs...).FirstErr()
}

// Close flushes and closes.
func (p *Producer) Close() { p.cl.Close() }

// Sink is where the consumer writes batches.
type Sink interface {
	Insert(ctx context.Context, events []event.Event, token string) error
}

// ConsumerConfig configures a consumer.
type ConsumerConfig struct {
	Brokers []string
	Topic   string
	Group   string
	// MaxPollRecords bounds records per poll (0 = unbounded).
	MaxPollRecords int
	// FailAfterInsert, if set, is called after a batch is inserted and before
	// offsets are committed; returning true simulates a crash at the worst
	// moment (tests only).
	FailAfterInsert func() bool
}

// Consumer reads events and writes them to a Sink, committing offsets only
// after the insert succeeded. A crash between insert and commit redelivers
// the batch, which the sink must tolerate (it does: see store).
type Consumer struct {
	cl       *kgo.Client
	sink     Sink
	cfg      ConsumerConfig
	Inserted atomic.Int64
	Batches  atomic.Int64
	Invalid  atomic.Int64
}

// NewConsumer joins the consumer group.
func NewConsumer(cfg ConsumerConfig, sink Sink) (*Consumer, error) {
	if cfg.Topic == "" {
		cfg.Topic = Topic
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumerGroup(cfg.Group), kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		return nil, err
	}
	return &Consumer{cl: cl, sink: sink, cfg: cfg}, nil
}

// ErrSimulatedCrash is returned when FailAfterInsert fires.
var ErrSimulatedCrash = errors.New("simulated crash after insert, before commit")

// Run consumes until ctx ends or an error occurs.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		var fetches kgo.Fetches
		if c.cfg.MaxPollRecords > 0 {
			fetches = c.cl.PollRecords(ctx, c.cfg.MaxPollRecords)
		} else {
			fetches = c.cl.PollFetches(ctx)
		}
		if ctx.Err() != nil {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("fetch: %v", errs[0].Err)
		}
		var err error
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if err != nil || len(p.Records) == 0 {
				return
			}
			evs := make([]event.Event, 0, len(p.Records))
			for _, r := range p.Records {
				var e event.Event
				if json.Unmarshal(r.Value, &e) != nil {
					c.Invalid.Add(1) // can't happen for records we produced; skip rather than wedge
					continue
				}
				evs = append(evs, e)
			}
			first, last := p.Records[0].Offset, p.Records[len(p.Records)-1].Offset
			token := fmt.Sprintf("%s/%d/%d-%d", p.Topic, p.Partition, first, last)
			if e := c.sink.Insert(ctx, evs, token); e != nil {
				err = fmt.Errorf("insert %s: %w", token, e)
				return
			}
			c.Inserted.Add(int64(len(evs)))
			c.Batches.Add(1)
		})
		if err != nil {
			return err
		}
		if c.cfg.FailAfterInsert != nil && c.cfg.FailAfterInsert() {
			return ErrSimulatedCrash
		}
		if err := c.cl.CommitUncommittedOffsets(ctx); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
}

// Close leaves the group.
func (c *Consumer) Close() { c.cl.Close() }

// RegisterMetrics exposes consumer counters.
func RegisterMetrics(reg prometheus.Registerer, c *Consumer) {
	reg.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "meterline_consumed_events_total", Help: "Events inserted by the consumer (incl. redeliveries)."},
			func() float64 { return float64(c.Inserted.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "meterline_consumed_batches_total", Help: "Batches inserted."},
			func() float64 { return float64(c.Batches.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "meterline_invalid_records_total", Help: "Undecodable records skipped."},
			func() float64 { return float64(c.Invalid.Load()) }),
	)
}
