// Package billing prices usage against plans. Money is exact decimal, never
// float; lines round to cents with banker's rounding.
package billing

import (
	"fmt"
	"sort"

	"github.com/shopspring/decimal"
)

// Tier is one graduated pricing band. UpTo is the cumulative billable
// quantity where the band ends (0 = no upper bound). Price is charged per
// PricePer units (e.g. $0.15 per 1,000,000 tokens).
type Tier struct {
	UpTo  int64           `json:"up_to" yaml:"up_to"`
	Price decimal.Decimal `json:"price" yaml:"price"`
}

// MeterPrice prices one meter.
type MeterPrice struct {
	Included int64  `json:"included" yaml:"included"`   // free units per period
	PricePer int64  `json:"price_per" yaml:"price_per"` // units the tier price applies to (default 1)
	Tiers    []Tier `json:"tiers" yaml:"tiers"`
}

// Plan maps meters to prices.
type Plan struct {
	Name     string                `json:"name" yaml:"name"`
	Currency string                `json:"currency" yaml:"currency"`
	Meters   map[string]MeterPrice `json:"meters" yaml:"meters"`
}

// Validate checks tier ordering and prices.
func (p Plan) Validate() error {
	for name, m := range p.Meters {
		if m.Included < 0 || m.PricePer < 0 {
			return fmt.Errorf("meter %s: included and price_per must be >= 0", name)
		}
		if len(m.Tiers) == 0 {
			return fmt.Errorf("meter %s: at least one tier required", name)
		}
		var prev int64
		for i, t := range m.Tiers {
			if t.Price.IsNegative() {
				return fmt.Errorf("meter %s tier %d: negative price", name, i)
			}
			last := i == len(m.Tiers)-1
			switch {
			case t.UpTo == 0 && !last:
				return fmt.Errorf("meter %s: only the last tier may be unbounded", name)
			case t.UpTo != 0 && t.UpTo <= prev:
				return fmt.Errorf("meter %s: tier bounds must increase", name)
			case last && t.UpTo != 0:
				return fmt.Errorf("meter %s: the last tier must be unbounded (up_to: 0)", name)
			}
			prev = t.UpTo
		}
	}
	return nil
}

// TierCharge is the part of a line billed in one tier.
type TierCharge struct {
	From     int64           `json:"from"`
	To       int64           `json:"to"` // inclusive upper bound of billable units in this tier
	Quantity int64           `json:"quantity"`
	Price    decimal.Decimal `json:"price"`
	PricePer int64           `json:"price_per"`
	Amount   decimal.Decimal `json:"amount"`
}

// Line is one meter on an invoice.
type Line struct {
	Meter     string          `json:"meter"`
	Quantity  int64           `json:"quantity"`
	Included  int64           `json:"included"`
	Billable  int64           `json:"billable"`
	Breakdown []TierCharge    `json:"breakdown,omitempty"`
	Amount    decimal.Decimal `json:"amount"`
	Unpriced  bool            `json:"unpriced,omitempty"`
}

// Usage is a meter quantity.
type Usage struct {
	Meter    string `json:"meter"`
	Quantity int64  `json:"quantity"`
}

// Invoice is a priced period.
type Invoice struct {
	Customer    string          `json:"customer"`
	Period      string          `json:"period"`
	Plan        string          `json:"plan"`
	Currency    string          `json:"currency"`
	Lines       []Line          `json:"lines"`
	Adjustments []Line          `json:"late_usage_adjustments"`
	Subtotal    decimal.Decimal `json:"subtotal"`
	Late        decimal.Decimal `json:"late_total"`
	Total       decimal.Decimal `json:"total"`
	Final       bool            `json:"final"`
}

// Price computes one line. Included units are free and come off the bottom;
// the remaining quantity walks the graduated tiers.
func Price(meter string, qty int64, mp *MeterPrice) Line {
	l := Line{Meter: meter, Quantity: qty, Amount: decimal.Zero}
	if mp == nil {
		l.Unpriced = true
		return l
	}
	per := mp.PricePer
	if per <= 0 {
		per = 1
	}
	l.Included = min(qty, mp.Included)
	l.Billable = qty - l.Included
	remaining, start := l.Billable, int64(0)
	for _, t := range mp.Tiers {
		if remaining <= 0 {
			break
		}
		width := remaining
		if t.UpTo > 0 {
			width = min(remaining, t.UpTo-start)
		}
		if width <= 0 {
			continue
		}
		amt := t.Price.Mul(decimal.NewFromInt(width)).Div(decimal.NewFromInt(per))
		l.Breakdown = append(l.Breakdown, TierCharge{From: start + 1, To: start + width, Quantity: width, Price: t.Price,
			PricePer: per, Amount: amt.RoundBank(6)})
		l.Amount = l.Amount.Add(amt)
		remaining -= width
		start += width
	}
	l.Amount = l.Amount.RoundBank(2)
	return l
}

// Build prices a period. usage is the on-time usage for the period. late is
// usage from the previous period that arrived after that period closed, and
// prevOnTime is that previous period's on-time usage: a late adjustment is
// priced marginally, as if it had arrived on time (price(prev+late) -
// price(prev)), so it uses whatever included quota and tier position the
// previous period actually had left.
func Build(customer, period string, plan Plan, usage, late, prevOnTime []Usage, final bool) Invoice {
	inv := Invoice{Customer: customer, Period: period, Plan: plan.Name, Currency: plan.Currency,
		Subtotal: decimal.Zero, Late: decimal.Zero, Final: final}
	sortUsage(usage)
	sortUsage(late)
	price := func(meter string, qty int64) Line {
		if mp, ok := plan.Meters[meter]; ok {
			return Price(meter, qty, &mp)
		}
		return Price(meter, qty, nil)
	}
	for _, u := range usage {
		l := price(u.Meter, u.Quantity)
		inv.Lines = append(inv.Lines, l)
		inv.Subtotal = inv.Subtotal.Add(l.Amount)
	}
	prev := map[string]int64{}
	for _, u := range prevOnTime {
		prev[u.Meter] += u.Quantity
	}
	for _, u := range late {
		before, after := price(u.Meter, prev[u.Meter]), price(u.Meter, prev[u.Meter]+u.Quantity)
		l := Line{Meter: u.Meter, Quantity: u.Quantity, Billable: after.Billable - before.Billable,
			Included: u.Quantity - (after.Billable - before.Billable), Amount: after.Amount.Sub(before.Amount),
			Unpriced: after.Unpriced}
		inv.Adjustments = append(inv.Adjustments, l)
		inv.Late = inv.Late.Add(l.Amount)
	}
	inv.Total = inv.Subtotal.Add(inv.Late)
	return inv
}

func sortUsage(u []Usage) { sort.Slice(u, func(i, j int) bool { return u[i].Meter < u[j].Meter }) }
