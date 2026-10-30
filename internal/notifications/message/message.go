// Package message formats the checked quote of a delivery attempt for Telegram.
package message

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
)

var ErrInvalidInput = errors.New("invalid notification message input")

// Input is a presentation projection of the subscription and the API attempt
// permission. ActualPrice, TradeTime and stream diagnostics MUST come from the
// permission's checked quote, never AlertTriggered.CandidateQuote. CheckedAt is
// the caller's clock reading for the displayed age; Format reads no clock.
// A stale trade is allowed. The caller owns permission validity and expiry.
type Input struct {
	Pair              string
	Threshold         price.Price
	Direction         price.Direction
	ActualPrice       price.Price
	TradeTime         time.Time
	CheckedAt         time.Time
	StreamUnconfirmed bool
	GapDetected       bool
}

// Format returns HTML text. The sender must use parse_mode=HTML without further
// escaping. Pair is a display label; symbol normalization belongs to the caller.
// This function neither grants permission nor checks the delivery condition.
func Format(in Input) (string, error) {
	if strings.TrimSpace(in.Pair) == "" || in.ActualPrice.Cmp(price.Price{}) <= 0 ||
		in.CheckedAt.IsZero() || in.CheckedAt.Year() < 1 || in.CheckedAt.Year() > 9999 {
		return "", ErrInvalidInput
	}
	freshness := price.CheckFreshness(true, in.TradeTime, in.CheckedAt)
	if freshness == price.InvalidTime {
		return "", ErrInvalidInput
	}
	var direction string
	switch in.Direction {
	case price.Up:
		direction = "вверх (≥)"
	case price.Down:
		direction = "вниз (≤)"
	default:
		return "", ErrInvalidInput
	}
	text := fmt.Sprintf("Пара: <b>%s</b>\nНаправление: %s\nПорог: %s\nФактическая цена: %s\nВремя сделки (UTC): %s\nВозраст сделки: %s с",
		htmlEscape.Replace(in.Pair), direction, in.Threshold.String(), in.ActualPrice.String(),
		in.TradeTime.UTC().Format(time.RFC3339Nano), ageSeconds(in.TradeTime, in.CheckedAt))
	if freshness == price.Stale {
		text += "\nПоследняя известная сделка была давно; цена может не отражать текущее состояние рынка"
	}
	if in.StreamUnconfirmed || in.GapDetected {
		text += "\nАктуальность рыночных данных не подтверждена; новые сделки могли быть пропущены"
	}
	return text, nil
}

var htmlEscape = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// Unix seconds cover the RFC3339 range without time.Duration saturation. Keep
// nanoseconds exact at the five-second warning boundary and for old trades.
func ageSeconds(trade, checked time.Time) string {
	seconds := checked.Unix() - trade.Unix()
	nanos := checked.Nanosecond() - trade.Nanosecond()
	if nanos < 0 {
		seconds--
		nanos += 1_000_000_000
	}
	if nanos == 0 {
		return fmt.Sprint(seconds)
	}
	return strings.TrimRight(fmt.Sprintf("%d.%09d", seconds, nanos), "0")
}
