package trade_test

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func metadata() trade.Metadata {
	return trade.Metadata{
		ReceivedAt: time.Date(2026, 10, 27, 12, 0, 0, 0, time.FixedZone("MSK", 3*3600)),
		Unit:       priceobserved.Milliseconds, Generation: 2, Sequence: 17,
		StreamState: priceobserved.Unconfirmed, GapDetected: true,
	}
}
func TestDecodeFixtures(t *testing.T) {
	for _, tc := range []struct {
		name, id, price, traded, event string
		unit                           priceobserved.TimeUnit
	}{
		{"trade.json", "12345", "0.001", "2022-12-31T19:43:02.001Z", "2022-12-31T19:43:02.136Z", priceobserved.Milliseconds},
		{"precise_trade.json", "18446744073709551615", "123456789012345678.123456789012345678", "2022-12-31T19:43:02.001123Z", "2022-12-31T19:43:02.136456Z", priceobserved.Microseconds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := metadata()
			meta.Unit = tc.unit
			got, err := trade.Decode(fixture(t, tc.name), meta)
			if err != nil {
				t.Fatal(err)
			}
			if err = got.Validate(); err != nil {
				t.Fatal(err)
			}
			if got.Pair != "BTCUSDT" || got.SourceID != tc.id || got.Price.String() != tc.price || got.SchemaVersion != 1 {
				t.Fatalf("wrong observation: %+v", got)
			}
			if got.TradeTime.Format(time.RFC3339Nano) != tc.traded || got.SourceEventTime == nil || got.SourceEventTime.Format(time.RFC3339Nano) != tc.event {
				t.Fatalf("wrong timestamps: %+v", got)
			}
			if !got.ReceivedAt.Equal(meta.ReceivedAt) || got.ReceivedAt.Location() != time.UTC || got.TradeTime.Location() != time.UTC {
				t.Fatal("timestamps must retain instants in UTC")
			}
			if got.Revision != (priceobserved.Revision{StreamID: trade.StreamID, Generation: 2, Sequence: 17}) || got.StreamState != meta.StreamState || !got.GapDetected {
				t.Fatal("metadata lost")
			}
			meta.ReceivedAt = meta.ReceivedAt.Add(time.Hour)
			meta.StreamState = priceobserved.Live
			meta.GapDetected = false
			late, err := trade.Decode(fixture(t, tc.name), meta)
			if err != nil || !late.TradeTime.Equal(got.TradeTime) || late.SourceID != got.SourceID || late.GapDetected || late.StreamState != priceobserved.Live {
				t.Fatalf("late reception changed trade: %+v %v", late, err)
			}
		})
	}
}
func TestDecodeRejectsMalformedTrade(t *testing.T) {
	base := fixture(t, "trade.json")
	for _, field := range []string{"e", "s", "t", "p", "T", "E"} {
		for _, replacement := range []string{"", "null"} {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(base, &fields); err != nil {
				t.Fatal(err)
			}
			if replacement == "" {
				delete(fields, field)
			} else {
				fields[field] = json.RawMessage(replacement)
			}
			b, _ := json.Marshal(fields)
			t.Run(field+"/"+replacement, func(t *testing.T) { assertInvalid(t, b, metadata(), trade.ErrInvalidTrade) })
		}
	}
	for _, tc := range []struct {
		field, value string
		want         error
	}{
		{"e", `"aggTrade"`, trade.ErrUnsupportedEvent}, {"e", `"bookTicker"`, trade.ErrUnsupportedEvent},
		{"s", `""`, trade.ErrInvalidTrade}, {"s", `"btcusdt"`, trade.ErrInvalidTrade},
		{"t", `9007199254740993.0`, trade.ErrInvalidTrade}, {"t", `-1`, trade.ErrInvalidTrade}, {"t", `18446744073709551616`, trade.ErrInvalidTrade}, {"t", `"123"`, trade.ErrInvalidTrade},
		{"p", `"0"`, trade.ErrInvalidTrade}, {"p", `"-1"`, trade.ErrInvalidTrade}, {"p", `"1e2"`, trade.ErrInvalidTrade}, {"p", `1.23`, trade.ErrInvalidTrade}, {"p", `"0.0000000000000000001"`, trade.ErrInvalidTrade}, {"p", `"garbage"`, trade.ErrInvalidTrade},
		{"T", `1.5`, trade.ErrInvalidTrade}, {"T", `"1672515782001"`, trade.ErrInvalidTrade}, {"T", `9223372036854775807`, trade.ErrInvalidTrade},
		{"E", `true`, trade.ErrInvalidTrade},
	} {
		t.Run(tc.field+"/"+tc.value, func(t *testing.T) {
			var fields map[string]json.RawMessage
			_ = json.Unmarshal(base, &fields)
			fields[tc.field] = json.RawMessage(tc.value)
			b, _ := json.Marshal(fields)
			assertInvalid(t, b, metadata(), tc.want)
		})
	}
	for _, b := range []string{`{`, `null`, `[]`, `{} {}`, `{"stream":"btcusdt@trade","data":` + string(base) + `}`} {
		assertInvalid(t, []byte(b), metadata(), trade.ErrInvalidTrade)
	}
}
func TestDecodeRejectsInvalidMetadata(t *testing.T) {
	for _, change := range []func(*trade.Metadata){
		func(m *trade.Metadata) { m.Unit = "" }, func(m *trade.Metadata) { m.Generation = 0 }, func(m *trade.Metadata) { m.Sequence = 0 }, func(m *trade.Metadata) { m.ReceivedAt = time.Time{} }, func(m *trade.Metadata) { m.StreamState = "" },
	} {
		m := metadata()
		change(&m)
		assertInvalid(t, fixture(t, "trade.json"), m, trade.ErrInvalidTrade)
	}
}
func assertInvalid(t *testing.T, b []byte, m trade.Metadata, want error) {
	t.Helper()
	got, err := trade.Decode(b, m)
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v; input %s", err, want, b)
	}
	if !reflect.DeepEqual(got, priceobserved.Observed{}) {
		t.Fatalf("partial result on failure: %+v", got)
	}
}
