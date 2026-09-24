// Package event defines usage events and parses the accepted input formats.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Event is one unit of metered usage. ID is the idempotency key: two events
// with the same ID are the same event, however many times they arrive.
type Event struct {
	ID       string    `json:"id"`
	Customer string    `json:"customer"`
	Meter    string    `json:"meter"`
	Value    int64     `json:"value"`
	TS       time.Time `json:"ts"`
}

// Limits on field sizes; they bound memory and ClickHouse row width.
const (
	MaxIDLen       = 128
	MaxNameLen     = 64
	MaxFutureSkew  = 5 * time.Minute
	MaxEventsBatch = 10_000
)

// Validate checks one event. now bounds how far in the future TS may be.
func (e Event) Validate(now time.Time) error {
	switch {
	case e.ID == "" || len(e.ID) > MaxIDLen:
		return fmt.Errorf("id must be 1-%d characters", MaxIDLen)
	case !validName(e.Customer):
		return fmt.Errorf("customer must be 1-%d characters of [A-Za-z0-9_.:-]", MaxNameLen)
	case !validName(e.Meter):
		return fmt.Errorf("meter must be 1-%d characters of [A-Za-z0-9_.:-]", MaxNameLen)
	case e.Value < 0:
		return errors.New("value must be >= 0")
	case e.TS.IsZero():
		return errors.New("ts is required")
	case e.TS.After(now.Add(MaxFutureSkew)):
		return errors.New("ts is in the future")
	}
	return nil
}

func validName(s string) bool {
	if s == "" || len(s) > MaxNameLen {
		return false
	}
	for _, r := range s {
		if !nameRune(r) {
			return false
		}
	}
	return true
}

func nameRune(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_.:-", r)
}

// modelMuxEvent is ModelMux's usage event, schema v1.
type modelMuxEvent struct {
	Version          int       `json:"v"`
	ID               string    `json:"id"`
	Time             time.Time `json:"ts"`
	Tenant           string    `json:"tenant"`
	Model            string    `json:"model"`
	PromptTokens     int64     `json:"prompt_tokens"`
	CompletionTokens int64     `json:"completion_tokens"`
	Cache            string    `json:"cache"`
}

// Parse decodes a batch body: a JSON array whose items are either generic
// events ({id, customer, meter, value, ts}) or ModelMux usage events (v1),
// which expand into two metered events: <model>.input_tokens and
// <model>.output_tokens, with ids "<id>:in" and "<id>:out" so the expansion is
// itself idempotent. ModelMux cache hits are not billed and are skipped.
func Parse(body []byte) ([]Event, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("body must be a JSON array: %w", err)
	}
	if len(raw) > MaxEventsBatch {
		return nil, fmt.Errorf("at most %d events per batch", MaxEventsBatch)
	}
	out := make([]Event, 0, len(raw))
	for i, r := range raw {
		var probe struct {
			Version int    `json:"v"`
			Tenant  string `json:"tenant"`
		}
		if err := json.Unmarshal(r, &probe); err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
		if probe.Version == 1 && probe.Tenant != "" {
			var m modelMuxEvent
			if err := json.Unmarshal(r, &m); err != nil {
				return nil, fmt.Errorf("event %d: %w", i, err)
			}
			if m.Cache == "exact" || m.Cache == "semantic" {
				continue
			}
			meter := sanitize(m.Model)
			out = append(out,
				Event{ID: m.ID + ":in", Customer: m.Tenant, Meter: meter + ".input_tokens", Value: m.PromptTokens, TS: m.Time},
				Event{ID: m.ID + ":out", Customer: m.Tenant, Meter: meter + ".output_tokens", Value: m.CompletionTokens, TS: m.Time})
			continue
		}
		var e Event
		if err := json.Unmarshal(r, &e); err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
		out = append(out, e)
	}
	return out, nil
}

// sanitize maps arbitrary model names ("groq/llama-3.1") onto meter-name
// characters.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_.-", r) {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	out := b.String()
	if len(out) > MaxNameLen-len(".output_tokens") {
		out = out[:MaxNameLen-len(".output_tokens")]
	}
	if out == "" {
		out = "unknown"
	}
	return out
}
