package billing

import (
	"testing"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

var tokens = MeterPrice{Included: 1_000_000, PricePer: 1_000_000, Tiers: []Tier{
	{UpTo: 10_000_000, Price: d("0.20")}, // first 10M billable at $0.20/M
	{UpTo: 0, Price: d("0.10")},          // then $0.10/M
}}

func TestPriceIncludedAndTiers(t *testing.T) {
	cases := []struct {
		qty    int64
		amount string
		tiers  int
	}{
		{0, "0", 0},
		{999_999, "0", 0},                     // within included
		{1_000_000, "0", 0},                   // exactly included
		{2_000_000, "0.20", 1},                // 1M billable at 0.20
		{11_000_000, "2", 1},                  // 10M billable, exactly at the tier bound
		{11_000_001, "2", 2},                  // 1 unit into tier 2 rounds to 0.00 extra
		{31_000_000, "4", 2},                  // 10M*0.20 + 20M*0.10
		{1_000_000 + 5_000_000_000, "501", 2}, // 10M@0.20 + 4.99B@0.10 = 2 + 499
	}
	for _, c := range cases {
		l := Price("m", c.qty, &tokens)
		if !l.Amount.Equal(d(c.amount)) || len(l.Breakdown) != c.tiers {
			t.Errorf("qty %d: amount %s (want %s), %d tiers (want %d)", c.qty, l.Amount, c.amount, len(l.Breakdown), c.tiers)
		}
		var sum int64
		for _, tc := range l.Breakdown {
			sum += tc.Quantity
		}
		if sum != l.Billable || l.Included+l.Billable != c.qty {
			t.Errorf("qty %d: breakdown doesn't add up", c.qty)
		}
	}
}

func TestBankersRoundingOnLines(t *testing.T) {
	mp := MeterPrice{Tiers: []Tier{{Price: d("0.005")}}}  // half a cent per unit
	if a := Price("m", 1, &mp).Amount; !a.Equal(d("0")) { // 0.005 -> 0.00 (round half to even)
		t.Fatalf("got %s", a)
	}
	if a := Price("m", 3, &mp).Amount; !a.Equal(d("0.02")) { // 0.015 -> 0.02
		t.Fatalf("got %s", a)
	}
}

func TestUnpricedMeter(t *testing.T) {
	if l := Price("x", 5, nil); !l.Unpriced || !l.Amount.IsZero() {
		t.Fatal(l)
	}
}

func TestBuildWithLateMarginalPricing(t *testing.T) {
	plan := Plan{Name: "pro", Currency: "usd", Meters: map[string]MeterPrice{"tok": tokens}}
	// Previous period used 500k (inside the 1M included). 700k arrived late:
	// 500k of it is still free, 200k is billable at 0.20/M = $0.04.
	inv := Build("acme", "2026-09", plan,
		[]Usage{{"tok", 2_000_000}, {"other", 3}},
		[]Usage{{"tok", 700_000}},
		[]Usage{{"tok", 500_000}}, false)
	if len(inv.Lines) != 2 || inv.Lines[0].Meter != "other" || !inv.Lines[0].Unpriced {
		t.Fatalf("lines: %+v", inv.Lines)
	}
	adj := inv.Adjustments[0]
	if !adj.Amount.Equal(d("0.04")) || adj.Billable != 200_000 || adj.Included != 500_000 {
		t.Fatalf("late adjustment: %+v", adj)
	}
	if !inv.Subtotal.Equal(d("0.2")) || !inv.Total.Equal(d("0.24")) {
		t.Fatalf("totals: %s %s", inv.Subtotal, inv.Total)
	}
}

func TestValidate(t *testing.T) {
	good := Plan{Meters: map[string]MeterPrice{"a": tokens}}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := []MeterPrice{
		{Tiers: nil},
		{Included: -1, Tiers: []Tier{{}}},
		{Tiers: []Tier{{UpTo: 0, Price: d("1")}, {UpTo: 0, Price: d("1")}}},
		{Tiers: []Tier{{UpTo: 10}, {UpTo: 5}, {}}},
		{Tiers: []Tier{{UpTo: 10}}},
		{Tiers: []Tier{{Price: d("-1")}}},
	}
	for i, m := range bad {
		if err := (Plan{Meters: map[string]MeterPrice{"m": m}}).Validate(); err == nil {
			t.Errorf("case %d accepted", i)
		}
	}
}
