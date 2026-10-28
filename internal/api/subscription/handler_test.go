package subscription_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type catalogFunc func(context.Context) (catalog.Snapshot, error)

func (f catalogFunc) Fetch(ctx context.Context) (catalog.Snapshot, error) { return f(ctx) }

type quoteFunc func(context.Context, string) (*pb.LatestResponse, error)

func (f quoteFunc) Latest(ctx context.Context, pair string) (*pb.LatestResponse, error) {
	return f(ctx, pair)
}

var createNow = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

func knownCatalog(context.Context) (catalog.Snapshot, error) {
	return catalog.Snapshot{Pairs: []catalog.Pair{{Symbol: "BTCUSDT"}, {Symbol: "RAREUSDT"}}}, nil
}
func createQuote(trade time.Time, stream string, gap bool) *pb.LatestResponse {
	return &pb.LatestResponse{Freshness: pb.Freshness_FRESHNESS_FRESH, Quote: &pb.Quote{Pair: "BTCUSDT", Price: "100", TradeTime: trade.Format(time.RFC3339Nano), StreamState: stream, GapDetected: gap}}
}
func createHTTP(t *testing.T, pool *pgxpool.Pool, owner string, c catalogFunc, q quoteFunc, now func() time.Time) (http.Handler, string) {
	t.Helper()
	sessions := session.New(pool, func() time.Time { return createNow })
	credentials, err := sessions.Issue(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	return auth.New(sessions, store.NewHandler(pool, c, q, now)), credentials.AccessToken
}
func postCreate(h http.Handler, token, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/subscriptions?user_id=attacker", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-User-ID", "attacker")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

type creationResponse struct {
	ID                string    `json:"id"`
	State             string    `json:"state"`
	Threshold         string    `json:"threshold"`
	Direction         string    `json:"direction"`
	CreationPrice     string    `json:"creation_price"`
	CreationTradeTime time.Time `json:"creation_trade_time"`
	Age               struct {
		Seconds int64 `json:"seconds"`
		Nanos   int32 `json:"nanos"`
	} `json:"age"`
	Warnings []string `json:"warnings"`
}

func TestCreateHTTPQuotes(t *testing.T) {
	pool := postgrestest.Open(t)
	for _, kind := range []string{"price", "percent"} {
		for _, tc := range []struct {
			name     string
			age      time.Duration
			stream   string
			gap      bool
			advance  time.Duration
			warnings string
		}{
			{"boundary", 5 * time.Second, "live", false, 0, ""},
			{"stale", 5*time.Second + time.Microsecond, "live", false, 0, "stale"},
			{"rare", 24 * time.Hour, "live", false, 0, "stale"},
			{"ages_before_use", 4 * time.Second, "live", false, 2 * time.Second, "stale"},
			{"gap", time.Second, "live", true, 0, "gap_detected"},
			{"restart", time.Second, "unconfirmed", false, 0, "stream_unconfirmed"},
			{"all", time.Hour, "unconfirmed", true, 0, "stale,stream_unconfirmed,gap_detected"},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				owner := quotaOwner(t, pool)
				now := createNow
				trade := now.Add(-tc.age)
				q := quoteFunc(func(_ context.Context, pair string) (*pb.LatestResponse, error) {
					now = now.Add(tc.advance)
					v := createQuote(trade, tc.stream, tc.gap)
					v.Quote.Pair = pair
					return v, nil
				})
				h, token := createHTTP(t, pool, owner, knownCatalog, q, func() time.Time { return now })
				body := `{"pair":"BTCUSDT","price":"90","direction":"up"}`
				want := "90"
				if kind == "percent" {
					body = `{"pair":"BTCUSDT","percent":"+5"}`
					want = "105"
				}
				if tc.name == "rare" {
					body = strings.ReplaceAll(body, "BTCUSDT", "RAREUSDT")
				}
				w := postCreate(h, token, body)
				if w.Code != 201 {
					t.Fatalf("%d %s", w.Code, w.Body.String())
				}
				var got creationResponse
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if got.Threshold != want || got.State != "active" || got.CreationPrice != "100" || !got.CreationTradeTime.Equal(trade) || strings.Join(got.Warnings, ",") != tc.warnings {
					t.Fatalf("response %+v", got)
				}
				age := tc.age + tc.advance
				if got.Age.Seconds != int64(age/time.Second) || got.Age.Nanos != int32(age%time.Second) {
					t.Fatalf("age %+v", got.Age)
				}
				saved, err := store.New(pool).ByID(context.Background(), owner, got.ID)
				if err != nil || saved.OwnerID != owner || saved.State != domain.Active || saved.Version != 1 || saved.Workflow != nil || saved.Blocked != nil || saved.Threshold.String() != want || saved.CreationPrice.String() != "100" || !saved.CreationTradeTime.Equal(trade) || saved.Activation != store.LatestPrice {
					t.Fatalf("saved %+v %v", saved, err)
				}
				if kind == "percent" && (saved.Percent == nil || *saved.Percent != "+5") {
					t.Fatal("percent not saved")
				}
				if saved.Stale != strings.Contains(tc.warnings, "stale") || saved.GapDetected != tc.gap || saved.StreamUnconfirmed != (tc.stream != "live") {
					t.Fatal("warnings not saved")
				}
				assertQuota(t, pool, owner, 1)
			})
		}
	}
}
func TestCreateHTTPRejectsWithoutWrites(t *testing.T) {
	pool := postgrestest.Open(t)
	for _, body := range []string{`{"pair":"BTCUSDT","price":"90","direction":"up"}`, `{"pair":"BTCUSDT","percent":"+5"}`} {
		for _, tc := range []struct {
			name       string
			quote      *pb.LatestResponse
			status     int
			code       string
			noTelegram bool
		}{
			{"missing", &pb.LatestResponse{Freshness: pb.Freshness_FRESHNESS_MISSING}, 503, "quote_unavailable", false},
			{"invalid_time", &pb.LatestResponse{Freshness: pb.Freshness_FRESHNESS_INVALID_TIME}, 503, "quote_unavailable", false},
			{"future", createQuote(createNow.Add(time.Microsecond), "live", false), 503, "quote_unavailable", false},
			{"absent_time", createQuote(time.Time{}, "live", false), 503, "quote_unavailable", false},
			{"no_telegram", createQuote(createNow.Add(-time.Hour), "live", false), 409, "telegram_not_linked", true},
		} {
			t.Run(tc.name+body, func(t *testing.T) {
				owner := quotaOwner(t, pool)
				if tc.noTelegram {
					if _, err := pool.Exec(context.Background(), `UPDATE app.users SET telegram_chat_id=NULL WHERE id=$1`, owner); err != nil {
						t.Fatal(err)
					}
				}
				h, token := createHTTP(t, pool, owner, knownCatalog, func(context.Context, string) (*pb.LatestResponse, error) { return tc.quote, nil }, func() time.Time { return createNow })
				w := postCreate(h, token, body)
				if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.code) {
					t.Fatalf("%d %s", w.Code, w.Body.String())
				}
				assertQuota(t, pool, owner, 0)
			})
		}
	}
}
func TestCreateHTTPInputAndDependencies(t *testing.T) {
	pool := postgrestest.Open(t)
	owner := quotaOwner(t, pool)
	q := quoteFunc(func(context.Context, string) (*pb.LatestResponse, error) {
		return createQuote(createNow, "live", false), nil
	})
	h, token := createHTTP(t, pool, owner, knownCatalog, q, func() time.Time { return createNow })
	for _, body := range []string{
		`{"pair":"BTCUSDT","user_id":"attacker","percent":"5"}`,
		`{"pair":"BTCUSDT","price":"100"}`, `{"pair":"BTCUSDT","price":"100","direction":"sideways"}`,
		`{"pair":"BTCUSDT","price":100,"direction":"up"}`, `{"pair":"BTCUSDT","percent":"0"}`,
		`{"pair":"BTCUSDT","percent":"-100"}`, `{"pair":"BTCUSDT","percent":"1e2"}`,
		`{"pair":"BTCUSDT","price":"100","direction":"up","percent":"5"}`,
		`{"pair":"BTCUSDT","percent":"5","direction":"down"}`,
		`{"pair":"BTCUSDT","price":"1.0000000000000000001","direction":"up"}`,
		`{"pair":"BTCUSDT","percent":"5"} {}`, `{`,
	} {
		w := postCreate(h, token, body)
		if w.Code != 400 {
			t.Fatalf("%s: %d %s", body, w.Code, w.Body.String())
		}
	}
	w := postCreate(h, token, `{"pair":"UNKNOWN","percent":"5"}`)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "unknown_pair") {
		t.Fatal(w.Code, w.Body.String())
	}
	w = postCreate(h, "", `{"pair":"BTCUSDT","percent":"5"}`)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	// Missing trusted context is rejected even if runtime forgets the wrapper.
	w = postCreate(store.NewHandler(pool, catalogFunc(knownCatalog), q, time.Now), token, `{"pair":"BTCUSDT","percent":"5"}`)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	for _, failure := range []string{"catalog", "quote", "empty_catalog"} {
		c := catalogFunc(knownCatalog)
		quotes := q
		if failure == "catalog" {
			c = func(context.Context) (catalog.Snapshot, error) {
				return catalog.Snapshot{}, errors.New("private source failure")
			}
		}
		if failure == "empty_catalog" {
			c = func(context.Context) (catalog.Snapshot, error) { return catalog.Snapshot{}, nil }
		}
		if failure == "quote" {
			quotes = func(context.Context, string) (*pb.LatestResponse, error) {
				return nil, errors.New("private quote failure")
			}
		}
		handler, tok := createHTTP(t, pool, owner, c, quotes, func() time.Time { return createNow })
		w := postCreate(handler, tok, `{"pair":"BTCUSDT","percent":"5"}`)
		want := 503
		if failure == "empty_catalog" {
			want = 400
		}
		if w.Code != want || strings.Contains(w.Body.String(), "private") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	assertQuota(t, pool, owner, 0)
}
func TestCreateHTTPQuotaAndDown(t *testing.T) {
	pool := postgrestest.Open(t)
	owner := quotaOwner(t, pool)
	h, token := createHTTP(t, pool, owner, knownCatalog, func(context.Context, string) (*pb.LatestResponse, error) {
		return createQuote(createNow, "live", false), nil
	}, func() time.Time { return createNow })
	for i := 0; i < 16; i++ {
		body := `{"pair":"BTCUSDT","percent":"-5"}`
		if i%2 == 1 {
			body = `{"pair":"BTCUSDT","price":"101","direction":"down"}`
		}
		w := postCreate(h, token, body)
		if i == 15 {
			if w.Code != 409 || !strings.Contains(w.Body.String(), "quota_exceeded") {
				t.Fatal(w.Code, w.Body.String())
			}
			break
		}
		var got creationResponse
		_ = json.Unmarshal(w.Body.Bytes(), &got)
		want := "95"
		if i%2 == 1 {
			want = "101"
		}
		if w.Code != 201 || got.Direction != "down" || got.Threshold != want {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	assertQuota(t, pool, owner, 15)
}

func TestCreateHTTPBindingChangeWhileWaiting(t *testing.T) {
	pool := postgrestest.Open(t)
	owner := quotaOwner(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	quoteRead := make(chan struct{})
	h, token := createHTTP(t, pool, owner, knownCatalog, func(context.Context, string) (*pb.LatestResponse, error) {
		close(quoteRead)
		return createQuote(createNow, "live", false), nil
	}, func() time.Time { return createNow })
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = store.LockQuota(ctx, tx, owner); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE app.users SET telegram_chat_id=NULL,telegram_version=telegram_version+1 WHERE id=$1`, owner); err != nil {
		t.Fatal(err)
	}
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- postCreate(h, token, `{"pair":"BTCUSDT","percent":"5"}`) }()
	select {
	case <-quoteRead:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Observe the HTTP transaction waiting for the real user lock, no timing guess.
	for {
		var waiting bool
		err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT id::text FROM app.users%')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case w := <-result:
		if w.Code != 409 || !strings.Contains(w.Body.String(), "telegram_not_linked") {
			t.Fatal(w.Code, w.Body.String())
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertQuota(t, pool, owner, 0)
}

func TestCreateHTTPTransportShape(t *testing.T) {
	h := store.NewHandler(nil, nil, nil, time.Now)
	for _, method := range []string{"GET", "DELETE"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, "/v1/subscriptions", nil))
		if w.Code != 405 {
			t.Fatal(fmt.Sprint(w.Code))
		}
	}
}

// The real transaction inserts successfully, but commit fails. The handler must
// neither return 201 nor leak the reserved slot or after-image on that path.
type failedCommitDB struct{ *pgxpool.Pool }
type failedCommitTx struct{ pgx.Tx }

func (db failedCommitDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := db.Pool.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return failedCommitTx{tx}, nil
}
func (tx failedCommitTx) Commit(context.Context) error { return errors.New("private commit failure") }
func TestCreateHTTPCommitFailure(t *testing.T) {
	pool := postgrestest.Open(t)
	owner := quotaOwner(t, pool)
	sessions := session.New(pool, func() time.Time { return createNow })
	credentials, err := sessions.Issue(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	q := quoteFunc(func(context.Context, string) (*pb.LatestResponse, error) {
		return createQuote(createNow, "live", false), nil
	})
	h := auth.New(sessions, store.NewHandler(failedCommitDB{pool}, catalogFunc(knownCatalog), q, func() time.Time { return createNow }))
	w := postCreate(h, credentials.AccessToken, `{"pair":"BTCUSDT","price":"90","direction":"up"}`)
	if w.Code != 500 || strings.Contains(w.Body.String(), "private") {
		t.Fatal(w.Code, w.Body.String())
	}
	assertQuota(t, pool, owner, 0)
}
