package message_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/message"
)

func parsed(t *testing.T, s string) price.Price {
	t.Helper()
	p, err := price.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func input(t *testing.T) message.Input {
	return message.Input{Pair: "BTCUSDT", Threshold: parsed(t, "100"), Direction: price.Up,
		ActualPrice: parsed(t, "101"), TradeTime: time.Date(2026, 10, 30, 12, 0, 0, 123456789, time.UTC),
		CheckedAt: time.Date(2026, 10, 30, 12, 0, 1, 123456789, time.UTC)}
}

func TestFormat(t *testing.T) {
	for _, tc := range []struct {
		name, pair, threshold, actual string
		direction                     price.Direction
		want                          string
	}{
		{"up", "BTCUSDT", "100", "101", price.Up, "Пара: <b>BTCUSDT</b>\nНаправление: вверх (≥)\nПорог: 100\nФактическая цена: 101"},
		{"down exact decimals", "ETHUSDT", "999999999999999999.999999999999999999", "0.000000000000000001", price.Down, "Пара: <b>ETHUSDT</b>\nНаправление: вниз (≤)\nПорог: 999999999999999999.999999999999999999\nФактическая цена: 0.000000000000000001"},
		{"escape", `A<&>"'_*[]()`, "0.1000", "0.100000000000000001", price.Up, `Пара: <b>A&lt;&amp;&gt;"'_*[]()</b>` + "\nНаправление: вверх (≥)\nПорог: 0.1\nФактическая цена: 0.100000000000000001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := input(t)
			in.Pair = tc.pair
			in.Threshold = parsed(t, tc.threshold)
			in.ActualPrice = parsed(t, tc.actual)
			in.Direction = tc.direction
			got, err := message.Format(in)
			want := tc.want + "\nВремя сделки (UTC): 2026-10-30T12:00:00.123456789Z\nВозраст сделки: 1 с"
			if err != nil || got != want {
				t.Fatalf("got %q, %v; want %q", got, err, want)
			}
		})
	}
}

func TestWarnings(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		age                             time.Duration
		unconfirmed, gap, stale, stream bool
	}{
		{"boundary", 5 * time.Second, false, false, false, false},
		{"one nanosecond stale", 5*time.Second + 1, false, false, true, false},
		{"unconfirmed fresh", time.Second, true, false, false, true},
		{"gap fresh", time.Second, false, true, false, true},
		{"both", time.Minute, true, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := input(t)
			in.CheckedAt = in.TradeTime.Add(tc.age)
			in.StreamUnconfirmed = tc.unconfirmed
			in.GapDetected = tc.gap
			got, err := message.Format(in)
			if err != nil {
				t.Fatal(err)
			}
			stale := "Последняя известная сделка была давно; цена может не отражать текущее состояние рынка"
			stream := "Актуальность рыночных данных не подтверждена; новые сделки могли быть пропущены"
			if strings.Contains(got, stale) != tc.stale || strings.Contains(got, stream) != tc.stream || strings.Count(got, stream) > 1 {
				t.Fatal(got)
			}
			if tc.age == 5*time.Second+1 && !strings.Contains(got, "Возраст сделки: 5.000000001 с") {
				t.Fatal(got)
			}
		})
	}
}

func TestInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*message.Input)
	}{
		{"pair", func(in *message.Input) { in.Pair = " " }},
		{"direction", func(in *message.Input) { in.Direction = "sideways" }},
		{"missing price", func(in *message.Input) { in.ActualPrice = price.Price{} }},
		{"missing time", func(in *message.Input) { in.TradeTime = time.Time{} }},
		{"future", func(in *message.Input) { in.TradeTime = in.CheckedAt.Add(time.Nanosecond) }},
		{"missing clock", func(in *message.Input) { in.CheckedAt = time.Time{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := input(t)
			tc.mutate(&in)
			got, err := message.Format(in)
			if !errors.Is(err, message.ErrInvalidInput) || got != "" {
				t.Fatalf("%q, %v", got, err)
			}
		})
	}
}

func TestTimeZoneAndLargeAge(t *testing.T) {
	in := input(t)
	want, err := message.Format(in)
	if err != nil {
		t.Fatal(err)
	}
	in.TradeTime = in.TradeTime.In(time.FixedZone("Moscow", 3*3600))
	in.CheckedAt = in.CheckedAt.In(time.FixedZone("Other", -5*3600))
	got, err := message.Format(in)
	if err != nil || got != want {
		t.Fatalf("%q, %v", got, err)
	}
	in.TradeTime = time.Date(2026, 10, 30, 12, 0, 0, 900000000, time.UTC)
	in.CheckedAt = time.Date(2026, 10, 30, 12, 0, 1, 100000000, time.UTC)
	got, err = message.Format(in)
	if err != nil || !strings.Contains(got, "Возраст сделки: 0.2 с") {
		t.Fatalf("%q, %v", got, err)
	}
	in.TradeTime = time.Date(1, 1, 1, 0, 0, 1, 0, time.UTC)
	in.CheckedAt = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err = message.Format(in)
	if err != nil || !strings.Contains(got, "Возраст сделки: 315506361599 с") {
		t.Fatalf("%q, %v", got, err)
	}
}
