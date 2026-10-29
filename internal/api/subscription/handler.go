package subscription

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/jackc/pgx/v5"
)

// Beginner is implemented by the API's PostgreSQL pool. The handler owns commit.
type Beginner interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// CatalogSource is implemented by catalogrpc.Client, not a Binance client in API.
type CatalogSource interface {
	Fetch(context.Context) (catalog.Snapshot, error)
}

// QuoteSource is implemented by latest.Client on an authenticated connection.
type QuoteSource interface {
	Latest(context.Context, string) (*pb.LatestResponse, error)
}

type createInput struct {
	Pair      string          `json:"pair"`
	Price     *string         `json:"price"`
	Direction price.Direction `json:"direction"`
	Percent   *string         `json:"percent"`
}
type quoteAge struct {
	Seconds int64 `json:"seconds"`
	Nanos   int32 `json:"nanos"`
}
type createResponse struct {
	ID                string          `json:"id"`
	Pair              string          `json:"pair"`
	State             domain.State    `json:"state"`
	Threshold         price.Price     `json:"threshold"`
	Direction         price.Direction `json:"direction"`
	Percent           *string         `json:"percent,omitempty"`
	CreationPrice     price.Price     `json:"creation_price"`
	CreationTradeTime time.Time       `json:"creation_trade_time"`
	Age               quoteAge        `json:"age"`
	Warnings          []string        `json:"warnings"`
}

// NewHandler mounts POST /v1/subscriptions. Wrap with auth.New; an absent trusted
// owner is always rejected. Sources must enforce finite RPC budgets. There is no
// automatic retry, runtime wiring or workflow in this adapter. Creation and its
// immutable journal/outbox event commit in the same transaction.
func NewHandler(db Beginner, catalogs CatalogSource, quotes QuoteSource, now func() time.Time) http.Handler {
	if now == nil {
		now = time.Now
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		owner, ok := auth.UserID(r.Context())
		if !ok {
			createError(w, 401, "invalid_session")
			return
		}
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			createError(w, 415, "unsupported_media_type")
			return
		}
		var in createInput
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&in); err != nil {
			createError(w, 400, "invalid_request")
			return
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			createError(w, 400, "invalid_request")
			return
		}
		record := Record{OwnerID: owner, Pair: in.Pair, State: domain.Active, Version: 1, Activation: LatestPrice}
		// Validate the mutually exclusive input forms before calling dependencies.
		if in.Pair == "" || (in.Price == nil) == (in.Percent == nil) {
			createError(w, 400, "invalid_request")
			return
		}
		if in.Price != nil {
			if in.Direction != price.Up && in.Direction != price.Down {
				createError(w, 400, "invalid_request")
				return
			}
			record.Threshold, err = price.Parse(*in.Price)
			record.Direction = in.Direction
		} else if in.Direction != "" {
			createError(w, 400, "invalid_request")
			return
		}
		if err != nil {
			createError(w, 400, "invalid_request")
			return
		}
		snapshot, err := catalogs.Fetch(r.Context())
		if err != nil {
			createError(w, 503, "catalog_unavailable")
			return
		}
		found := false
		for _, pair := range snapshot.Pairs {
			if pair.Symbol == in.Pair {
				found = true
				break
			}
		}
		if !found {
			createError(w, 400, "unknown_pair")
			return
		}
		quote, err := quotes.Latest(r.Context(), in.Pair)
		if err != nil || quote == nil || quote.Quote == nil || (quote.Freshness != pb.Freshness_FRESHNESS_FRESH && quote.Freshness != pb.Freshness_FRESHNESS_STALE) {
			createError(w, 503, "quote_unavailable")
			return
		}
		q := quote.Quote
		record.CreationPrice, err = price.Parse(q.Price)
		if err != nil || record.CreationPrice.Cmp(price.Price{}) <= 0 || q.Pair != in.Pair {
			createError(w, 503, "quote_unavailable")
			return
		}
		record.CreationTradeTime, err = time.Parse(time.RFC3339Nano, q.TradeTime)
		if err != nil {
			createError(w, 503, "quote_unavailable")
			return
		}
		record.CreationTradeTime = record.CreationTradeTime.UTC()
		if in.Percent != nil {
			threshold, err := price.NewPercentageThreshold(record.CreationPrice, *in.Percent)
			if err != nil {
				createError(w, 400, "invalid_request")
				return
			}
			record.Threshold, record.Direction, record.Percent = threshold.Threshold, threshold.Direction, &threshold.Percent
		}
		// Validate before starting a transaction, then recheck at actual use after
		// acquiring the owner lock. No network I/O takes place under that lock.
		if !usableTime(record.CreationTradeTime, now()) {
			createError(w, 503, "quote_unavailable")
			return
		}
		record.ID = newUUID()
		// Assigned once per command, before any journal write. Publication must
		// reuse these persisted metadata; this handler never retries creation.
		eventID := newUUID()
		tx, err := db.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			createError(w, 500, "internal_error")
			return
		}
		defer tx.Rollback(context.Background())
		quota, err := LockQuota(r.Context(), tx, owner)
		if err != nil {
			createError(w, 500, "internal_error")
			return
		}
		checked := now().UTC()
		if !usableTime(record.CreationTradeTime, checked) {
			createError(w, 503, "quote_unavailable")
			return
		}
		record.Stale = price.CheckFreshness(true, record.CreationTradeTime, checked) == price.Stale
		record.StreamUnconfirmed = q.StreamState != "live"
		record.GapDetected = q.GapDetected
		if err = quota.Create(r.Context(), record); err != nil {
			switch {
			case errors.Is(err, ErrTelegramNotLinked):
				createError(w, 409, "telegram_not_linked")
			case errors.Is(err, ErrQuotaExceeded):
				createError(w, 409, "quota_exceeded")
			default:
				createError(w, 500, "internal_error")
			}
			return
		}
		// Use the actual persisted after-image, including PostgreSQL timestamp
		// precision. All business reads/writes finish before the journal lock.
		record, err = New(tx).ByID(r.Context(), owner, record.ID)
		if err != nil {
			createError(w, 500, "internal_error")
			return
		}
		_, err = journal.Append(r.Context(), tx, []journal.Entry{{
			Metadata: event.Envelope{EventID: eventID, Type: changed.Type, Version: event.Version1,
				OccurredAt: checked, CorrelationID: eventID, OrderingKey: record.ID},
			Change: changed.Changed{SchemaVersion: changed.Version2, OwnerID: record.OwnerID, SubscriptionID: record.ID,
				SubscriptionVersion: uint64(record.Version), State: record.State, Pair: record.Pair,
				Threshold: &record.Threshold, Direction: record.Direction, Activation: record.Activation,
				CreationSnapshot: &changed.Snapshot{Price: &record.CreationPrice, TradeTime: record.CreationTradeTime}},
		}})
		if err != nil {
			createError(w, 500, "internal_error")
			return
		}
		if err = tx.Commit(r.Context()); err != nil {
			createError(w, 500, "internal_error")
			return
		}
		createJSON(w, http.StatusCreated, creationResult(record, checked))
	})
	return mux
}

// creationResult keeps warning order and exact age representation in one place.
func creationResult(record Record, checked time.Time) createResponse {
	warnings := make([]string, 0, 3)
	if record.Stale {
		warnings = append(warnings, "stale")
	}
	if record.StreamUnconfirmed {
		warnings = append(warnings, "stream_unconfirmed")
	}
	if record.GapDetected {
		warnings = append(warnings, "gap_detected")
	}
	seconds := checked.Unix() - record.CreationTradeTime.Unix()
	nanos := checked.Nanosecond() - record.CreationTradeTime.Nanosecond()
	if nanos < 0 {
		seconds--
		nanos += 1000000000
	}
	return createResponse{ID: record.ID, Pair: record.Pair, State: record.State, Threshold: record.Threshold, Direction: record.Direction, Percent: record.Percent, CreationPrice: record.CreationPrice, CreationTradeTime: record.CreationTradeTime, Age: quoteAge{Seconds: seconds, Nanos: int32(nanos)}, Warnings: warnings}
}

func usableTime(trade, now time.Time) bool {
	fresh := price.CheckFreshness(true, trade, now)
	return fresh == price.Fresh || fresh == price.Stale
}
func createError(w http.ResponseWriter, status int, code string) {
	createJSON(w, status, map[string]map[string]string{"error": {"code": code}})
}
func createJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// newUUID creates independent subscription/event identities once per command.
func newUUID() string {
	raw := make([]byte, 16)
	rand.Read(raw)
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[:4], raw[4:6], raw[6:8], raw[8:10], raw[10:])
}
