package event

import (
	"strings"
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	ok := Event{ID: "e1", Customer: "acme", Meter: "api.calls", Value: 3, TS: now}
	if err := ok.Validate(now); err != nil {
		t.Fatal(err)
	}
	bad := []Event{
		{Customer: "acme", Meter: "m", TS: now},
		{ID: strings.Repeat("x", 129), Customer: "acme", Meter: "m", TS: now},
		{ID: "e", Customer: "ac me", Meter: "m", TS: now},
		{ID: "e", Customer: "acme", Meter: "", TS: now},
		{ID: "e", Customer: "acme", Meter: "m", Value: -1, TS: now},
		{ID: "e", Customer: "acme", Meter: "m"},
		{ID: "e", Customer: "acme", Meter: "m", TS: now.Add(time.Hour)},
	}
	for i, e := range bad {
		if e.Validate(now) == nil {
			t.Errorf("case %d accepted", i)
		}
	}
}

func TestParseGenericAndModelMux(t *testing.T) {
	body := `[
	  {"id":"a","customer":"acme","meter":"api.calls","value":1,"ts":"2026-09-24T10:00:00Z"},
	  {"v":1,"id":"mm1","ts":"2026-09-24T10:00:00Z","tenant":"acme","model":"groq/llama 3","prompt_tokens":12,"completion_tokens":30,"cache":"miss"},
	  {"v":1,"id":"mm2","ts":"2026-09-24T10:00:00Z","tenant":"acme","model":"fast","prompt_tokens":5,"completion_tokens":5,"cache":"exact"}
	]`
	evs, err := Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 {
		t.Fatalf("want 3 events (generic + 2 expanded, cache hit skipped), got %d: %+v", len(evs), evs)
	}
	if evs[1].ID != "mm1:in" || evs[1].Meter != "groq_llama_3.input_tokens" || evs[1].Value != 12 ||
		evs[2].ID != "mm1:out" || evs[2].Value != 30 || evs[2].Customer != "acme" {
		t.Fatalf("expansion: %+v", evs[1:])
	}
	for _, e := range evs {
		if err := e.Validate(time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)); err != nil {
			t.Fatalf("expanded event invalid: %v", err)
		}
	}
	for _, bad := range []string{`{}`, `[1]`, `[{"id":5}]`, `[{"v":1,"tenant":"x","prompt_tokens":"no"}]`} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
	big := "[" + strings.Repeat(`{},`, MaxEventsBatch) + "{}]"
	if _, err := Parse([]byte(big)); err == nil {
		t.Fatal("oversized batch accepted")
	}
	if sanitize("") != "unknown" || len(sanitize(strings.Repeat("a", 200))) > MaxNameLen {
		t.Fatal("sanitize")
	}
}
