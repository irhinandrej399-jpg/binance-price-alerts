package subscription_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"strconv"
)

func TestCreateHTTPJournalCommit(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for _, body := range []string{`{"pair":"BTCUSDT","price":"999999999999999999.123456789012345678","direction":"down"}`, `{"pair":"BTCUSDT","percent":"+5"}`} {
		owner := quotaOwner(t, pool)
		q := quoteFunc(func(context.Context, string) (*pb.LatestResponse, error) {
			return createQuote(createNow.Add(-123456789*time.Nanosecond), "live", false), nil
		})
		h, token := createHTTP(t, pool, owner, knownCatalog, q, func() time.Time { return createNow })
		before, err := journal.Current(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		w := postCreate(h, token, body)
		if w.Code != http.StatusCreated {
			t.Fatal(w.Code, w.Body.String())
		}
		var response creationResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		saved, err := store.New(pool).ByID(ctx, owner, response.ID)
		if err != nil {
			t.Fatal(err)
		}
		entries, err := journal.ReadAfter(ctx, pool, before, 1)
		if err != nil || len(entries) != 1 {
			t.Fatalf("journal len=%d err=%v", len(entries), err)
		}
		e := entries[0]
		c := e.Change
		if c.SchemaVersion != 2 || c.OwnerID != saved.OwnerID || c.SubscriptionID != saved.ID || c.SubscriptionVersion != uint64(saved.Version) || c.State != saved.State || c.Pair != saved.Pair || c.Threshold.String() != saved.Threshold.String() || c.Direction != saved.Direction || c.Activation != saved.Activation || c.Workflow != nil || c.CreationSnapshot.Price.String() != saved.CreationPrice.String() || !c.CreationSnapshot.TradeTime.Equal(saved.CreationTradeTime) || c.Position != before+1 || c.TransactionEnd != before+1 {
			t.Fatalf("after-image differs: %+v vs %+v", c, saved)
		}
		if e.Metadata.EventID == "" || e.Metadata.CorrelationID == "" || e.Metadata.OrderingKey != saved.ID || !e.Metadata.OccurredAt.Equal(createNow) {
			t.Fatalf("metadata %+v", e.Metadata)
		}
		var first, second []byte
		for _, dest := range []*[]byte{&first, &second} {
			if err := pool.QueryRow(ctx, `SELECT body FROM app.api_journal WHERE event_id=$1`, e.Metadata.EventID).Scan(dest); err != nil {
				t.Fatal(err)
			}
		}
		meta, _, err := changed.Decode(second)
		if err != nil || !bytes.Equal(first, second) || meta.EventID != e.Metadata.EventID {
			t.Fatal("unstable stored event", err)
		}

		// An empty complete snapshot at the observed C precedes this actual API event.
		// Nonempty snapshot replacement is covered by projection replay tests.
		matcher := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
		snapshotID := "owner-" + saved.ID
		if _, err := matcher.Exec(ctx, `INSERT INTO app.matcher_snapshot_generations(snapshot_id,boundary_c,expires_at,expires_text,complete) VALUES($1,$2::numeric,now()+interval '1 hour','fixture',true)`, snapshotID, strconv.FormatUint(before, 10)); err != nil {
			t.Fatal(err)
		}
		sql := projection.New(matcher, snapshotID+"-sql")
		kafka := projection.New(matcher, snapshotID+"-kafka")
		for _, st := range []*projection.Store{sql, kafka} {
			if err := st.Initialize(ctx, snapshotID); err != nil {
				t.Fatal(err)
			}
		}
		if err := sql.ApplyReplay(ctx, [][]byte{first}); err != nil {
			t.Fatal(err)
		}
		if err := kafka.Handle(ctx, projection.Batch{Topic: "changes", NextOffset: 47, Events: [][]byte{first}}, func(context.Context, int64) error { return nil }); err != nil {
			t.Fatal(err)
		}
		reopened := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
		for _, id := range []string{snapshotID + "-sql", snapshotID + "-kafka"} {
			im, err := projection.New(reopened, id).ReadImage(ctx, saved.ID)
			if err != nil {
				t.Fatal(err)
			}
			if owner, err := im.OwnerID(); err != nil || owner != saved.OwnerID {
				t.Fatal(owner, err)
			}
		}
		if c, err := journal.Current(ctx, pool); err != nil || c != before+1 {
			t.Fatal(c, err)
		}
		assertQuota(t, pool, owner, 1)
	}
}

// Fail the actual journal INSERT after counter/batch and business writes. The
// server aborts the real transaction; no mocked success or independent write.
type failedJournalDB struct {
	*pgxpool.Pool
	t     *testing.T
	owner string
}
type failedJournalTx struct {
	pgx.Tx
	t     *testing.T
	owner string
}

func (db failedJournalDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := db.Pool.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return failedJournalTx{tx, db.t, db.owner}, nil
}
func (tx failedJournalTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "INSERT INTO app.api_journal (") {
		var count, reserved int
		if err := tx.QueryRow(ctx, `SELECT active_subscriptions,(SELECT count(*) FROM app.subscriptions WHERE owner_id=$1 AND quota_reserved) FROM app.users WHERE id=$1`, tx.owner).Scan(&count, &reserved); err != nil || count != 1 || reserved != 1 {
			tx.t.Fatalf("expected provisional creation/slot: %d %d %v", count, reserved, err)
		}
		return tx.Tx.Exec(ctx, `SELECT 1/0`)
	}
	return tx.Tx.Exec(ctx, sql, args...)
}
func TestCreateHTTPJournalFailure(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for _, failure := range []string{"journal", "commit"} {
		t.Run(failure, func(t *testing.T) {
			owner := quotaOwner(t, pool)
			sessions := session.New(pool, func() time.Time { return createNow })
			credentials, err := sessions.Issue(ctx, owner)
			if err != nil {
				t.Fatal(err)
			}
			var db store.Beginner = failedJournalDB{pool, t, owner}
			if failure == "commit" {
				db = failedCommitDB{pool}
			}
			q := quoteFunc(func(context.Context, string) (*pb.LatestResponse, error) {
				return createQuote(createNow, "live", false), nil
			})
			h := auth.New(sessions, store.NewHandler(db, catalogFunc(knownCatalog), q, func() time.Time { return createNow }))
			before, err := journal.Current(ctx, pool)
			if err != nil {
				t.Fatal(err)
			}
			w := postCreate(h, credentials.AccessToken, `{"pair":"BTCUSDT","price":"90","direction":"up"}`)
			if w.Code != 500 || strings.Contains(w.Body.String(), "division") || strings.Contains(w.Body.String(), "private") {
				t.Fatal(w.Code, w.Body.String())
			}
			assertQuota(t, pool, owner, 0)
			if c, err := journal.Current(ctx, pool); err != nil || c != before {
				t.Fatalf("counter leaked: %d %v", c, err)
			}
			if entries, err := journal.ReadAfter(ctx, pool, before, 1); err != nil || len(entries) != 0 {
				t.Fatalf("journal leaked: %d %v", len(entries), err)
			}
			var batches int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM app.api_journal_batches WHERE transaction_end>$1`, int64(before)).Scan(&batches); err != nil || batches != 0 {
				t.Fatal("batch leaked", batches, err)
			}
		})
	}
}
