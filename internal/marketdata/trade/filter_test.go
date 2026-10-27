package trade_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
)

func quote(t *testing.T, id string) priceobserved.Observed {
	t.Helper()
	q, err := trade.Decode([]byte(`{"e":"trade","s":"BTCUSDT","t":`+id+`,"p":"12.50","T":1700000000000,"E":1700000000001}`), trade.Metadata{
		Unit: priceobserved.Milliseconds, ReceivedAt: time.Unix(1700000001, 0), Generation: 1, Sequence: 1, StreamState: priceobserved.Unconfirmed,
	})
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestFilterDuplicatesOlderAndReconnect(t *testing.T) {
	var current *priceobserved.Observed
	for i, step := range []struct {
		id     string
		accept bool
	}{{"10", true}, {"10", false}, {"9", false}, {"11", true}, {"10", false}, {"11", false}, {"12", true}} {
		next := quote(t, step.id)
		if i >= 4 {
			next.Revision.Generation = 2
			next.ReceivedAt = next.ReceivedAt.Add(time.Hour)
		}
		before := next
		got, err := trade.CheckNext(current, next)
		if err != nil || got.Accept != step.accept || got.GapDetected {
			t.Fatalf("step %d: %+v, %v", i, got, err)
		}
		if !reflect.DeepEqual(before, next) {
			t.Fatal("candidate mutated")
		}
		if got.Accept {
			current = &next
		}
	}
}

func TestFilterGapAndNumericBoundaries(t *testing.T) {
	for _, tc := range []struct {
		prev, next  string
		accept, gap bool
	}{
		{"9", "10", true, false}, {"10", "12", true, true}, {"9007199254740992", "9007199254740993", true, false},
		{"18446744073709551614", "18446744073709551615", true, false},
		{"18446744073709551615", "0", false, false}, {"0", "1", true, false},
	} {
		t.Run(tc.prev+"/"+tc.next, func(t *testing.T) {
			prev, next := quote(t, tc.prev), quote(t, tc.next)
			got, err := trade.CheckNext(&prev, next)
			if err != nil || got.Accept != tc.accept || got.GapDetected != tc.gap {
				t.Fatalf("%+v, %v", got, err)
			}
		})
	}
	prev, next := quote(t, "10"), quote(t, "11")
	prev.GapDetected = true
	got, err := trade.CheckNext(&prev, next)
	if err != nil || !got.Accept || !got.GapDetected {
		t.Fatalf("historical gap lost: %+v %v", got, err)
	}
	next.GapDetected = true
	got, err = trade.CheckNext(nil, next)
	if err != nil || !got.Accept || !got.GapDetected {
		t.Fatalf("initial gap lost: %+v %v", got, err)
	}
}

func TestFilterRejectsInvalidOrIncomparableObservations(t *testing.T) {
	for _, mutate := range []func(*priceobserved.Observed){
		func(q *priceobserved.Observed) { q.Pair = "ETHUSDT" },
		func(q *priceobserved.Observed) { q.Revision.StreamID = "aggTrade" },
		func(q *priceobserved.Observed) { q.Price = nil },
		func(q *priceobserved.Observed) { q.SourceID = "" },
		func(q *priceobserved.Observed) { q.SourceID = "-1" },
		func(q *priceobserved.Observed) { q.SourceID = "+11" },
		func(q *priceobserved.Observed) { q.SourceID = "011" },
		func(q *priceobserved.Observed) { q.SourceID = "18446744073709551616" },
	} {
		for _, badPrevious := range []bool{false, true} {
			prev, next := quote(t, "10"), quote(t, "11")
			if badPrevious {
				mutate(&prev)
			} else {
				mutate(&next)
			}
			got, err := trade.CheckNext(&prev, next)
			if !errors.Is(err, trade.ErrInvalidFilterInput) || got != (trade.Decision{}) {
				t.Fatalf("invalid input: %+v %v", got, err)
			}
		}
	}
	bad := quote(t, "1")
	bad.Revision.StreamID = "aggTrade"
	if _, err := trade.CheckNext(nil, bad); !errors.Is(err, trade.ErrInvalidFilterInput) {
		t.Fatal(err)
	}
}

func TestFilterDoesNotAdvanceUncommittedBoundary(t *testing.T) {
	prev, next := quote(t, "10"), quote(t, "11")
	for range 2 { // A failed durable commit can retry the same candidate.
		got, err := trade.CheckNext(&prev, next)
		if err != nil || !got.Accept || prev.SourceID != "10" {
			t.Fatalf("%+v %v", got, err)
		}
	}
}
