// Package snapshotload loads immutable API snapshot pages into an isolated
// durable matcher generation. Completion never implies matcher readiness.
package snapshotload

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidSnapshot = errors.New("invalid or inconsistent subscription snapshot")
	ErrIncomplete      = errors.New("snapshot generation is incomplete")
	uuid               = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	requestID          = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
	pair               = regexp.MustCompile(`^[A-Z0-9]+$`)
)

// Source is implemented by snapshotrpc.Client, using caller-owned mTLS and
// finite per-call deadlines. Errors propagate; there are no hidden retries.
type Source interface {
	BuildSnapshot(context.Context, *pb.BuildSnapshotRequest) (*pb.SnapshotPageResponse, error)
	ReadSnapshotPage(context.Context, *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error)
}
type Generation struct {
	SnapshotID string
	C          uint64
	ExpiresAt  time.Time
	Count      int64
	Complete   bool // Snapshot only; replay and latest prices are still required.
}
type Loader struct {
	pool   *pgxpool.Pool
	source Source
}

func New(pool *pgxpool.Pool, source Source) *Loader { return &Loader{pool: pool, source: source} }

// Load always starts at BuildSnapshot with the same caller-persisted request ID
// and TTL after ambiguity/restart. Previously committed identical rows are safe
// repeats. No cursor is persisted. A lost/expired resource requires a new ID;
// the old partial generation is never eligible for replay or activation.
func (l *Loader) Load(ctx context.Context, r *pb.BuildSnapshotRequest) (Generation, error) {
	var zero Generation
	if r == nil || !requestID.MatchString(r.RequestId) || r.TtlMillis < 1 || r.TtlMillis > 86400000 || r.PageSize < 1 || r.PageSize > 1000 {
		return zero, ErrInvalidSnapshot
	}
	p, err := l.source.BuildSnapshot(ctx, proto.Clone(r).(*pb.BuildSnapshotRequest))
	if err != nil {
		return zero, err
	}
	var identity *pb.SnapshotPageResponse
	lastID := ""
	seen := map[string]bool{}
	var count int64
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if err := validatePage(p, identity, lastID, r.PageSize); err != nil {
			return zero, err
		}
		if identity == nil {
			identity = &pb.SnapshotPageResponse{SnapshotId: p.SnapshotId, BoundaryC: p.BoundaryC, ExpiresAt: p.ExpiresAt}
		}
		if p.NextCursor != "" && seen[p.NextCursor] {
			return zero, ErrInvalidSnapshot
		}
		if len(p.Subscriptions) > 0 {
			lastID = p.Subscriptions[len(p.Subscriptions)-1].SubscriptionId
		}
		count += int64(len(p.Subscriptions))
		g, err := l.save(ctx, p, count)
		if err != nil {
			return zero, err
		}
		if p.NextCursor == "" {
			return g, nil
		}
		seen[p.NextCursor] = true
		p, err = l.source.ReadSnapshotPage(ctx, &pb.ReadSnapshotPageRequest{SnapshotId: identity.SnapshotId, Cursor: p.NextCursor, PageSize: r.PageSize})
		if err != nil {
			return zero, err
		}
	}
}
func validatePage(p, identity *pb.SnapshotPageResponse, last string, size int32) error {
	if p == nil || !uuid.MatchString(p.SnapshotId) || len(p.Subscriptions) > int(size) || (p.NextCursor != "" && len(p.Subscriptions) == 0) {
		return ErrInvalidSnapshot
	}
	expiry, err := time.Parse(time.RFC3339Nano, p.ExpiresAt)
	if err != nil || !time.Now().Before(expiry) {
		return ErrInvalidSnapshot
	}
	if identity != nil && (p.SnapshotId != identity.SnapshotId || p.BoundaryC != identity.BoundaryC || p.ExpiresAt != identity.ExpiresAt) {
		return ErrInvalidSnapshot
	}
	for _, r := range p.Subscriptions {
		if !validRow(r) || r.SubscriptionId <= last {
			return ErrInvalidSnapshot
		}
		last = r.SubscriptionId
	}
	return nil
}
func validRow(r *pb.SnapshotSubscription) bool {
	if r == nil || !uuid.MatchString(r.SubscriptionId) || !uuid.MatchString(r.OwnerId) || r.Version < 1 || !pair.MatchString(r.Pair) || r.Activation != "latest_price" || (r.Direction != "up" && r.Direction != "down") {
		return false
	}
	if _, err := price.Parse(r.Threshold); err != nil {
		return false
	}
	if _, err := price.Parse(r.CreationPrice); err != nil {
		return false
	}
	tm, err := time.Parse(time.RFC3339Nano, r.CreationTradeTime)
	if err != nil || tm.IsZero() {
		return false
	}
	switch r.State {
	case "active", "pending_delivery", "sending", "delivery_blocked":
	default:
		return false
	}
	if r.Workflow != nil && (!uuid.MatchString(r.Workflow.TriggerId) || r.Workflow.Version < 1) {
		return false
	}
	if (r.State == "pending_delivery" || r.State == "sending") && r.Workflow == nil {
		return false
	}
	if r.Blocked != nil {
		if r.State != "delivery_blocked" || r.Blocked.BindingVersion < 0 {
			return false
		}
		switch r.Blocked.Reason {
		case "channel_unavailable", "attempts_exhausted", "permanent_failure":
		default:
			return false
		}
	}
	if r.State == "delivery_blocked" && r.Blocked == nil {
		return false
	}
	// Preserve signed audit percent as supplied; validate its decimal grammar.
	if r.Percent != nil {
		v := *r.Percent
		if len(v) > 0 && (v[0] == '+' || v[0] == '-') {
			v = v[1:]
		}
		if _, err := price.Parse(v); err != nil {
			return false
		}
	}
	return true
}

func (l *Loader) save(ctx context.Context, p *pb.SnapshotPageResponse, count int64) (Generation, error) {
	var zero Generation
	expiry, _ := time.Parse(time.RFC3339Nano, p.ExpiresAt)
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return zero, err
	}
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, `INSERT INTO app.matcher_snapshot_generations(snapshot_id,boundary_c,expires_at,expires_text) VALUES($1,$2::numeric,$3,$4) ON CONFLICT DO NOTHING`, p.SnapshotId, strconv.FormatUint(p.BoundaryC, 10), expiry, p.ExpiresAt)
	if err != nil {
		return zero, err
	}
	var c, expires string
	var complete bool
	var rows int64
	err = tx.QueryRow(ctx, `SELECT boundary_c::text,expires_text,complete,row_count FROM app.matcher_snapshot_generations WHERE snapshot_id=$1 FOR UPDATE`, p.SnapshotId).Scan(&c, &expires, &complete, &rows)
	if err != nil {
		return zero, err
	}
	if c != strconv.FormatUint(p.BoundaryC, 10) || expires != p.ExpiresAt {
		return zero, ErrInvalidSnapshot
	}
	for _, r := range p.Subscriptions {
		body, err := (proto.MarshalOptions{Deterministic: true}).Marshal(r)
		if err != nil {
			return zero, err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO app.matcher_snapshot_rows(snapshot_id,subscription_id,body) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, p.SnapshotId, r.SubscriptionId, body)
		if err != nil {
			return zero, err
		}
		if tag.RowsAffected() == 0 {
			var existing []byte
			if err := tx.QueryRow(ctx, `SELECT body FROM app.matcher_snapshot_rows WHERE snapshot_id=$1 AND subscription_id=$2`, p.SnapshotId, r.SubscriptionId).Scan(&existing); err != nil {
				return zero, err
			}
			prev := new(pb.SnapshotSubscription)
			if proto.Unmarshal(existing, prev) != nil || !proto.Equal(prev, r) {
				return zero, ErrInvalidSnapshot
			}
		} else {
			if complete {
				return zero, ErrInvalidSnapshot
			}
			rows++
		}
	}
	if p.NextCursor == "" && rows != count {
		return zero, ErrInvalidSnapshot
	}
	if !time.Now().Before(expiry) {
		return zero, ErrInvalidSnapshot
	}
	// Never regress completion when an earlier page is retried.
	_, err = tx.Exec(ctx, `UPDATE app.matcher_snapshot_generations SET row_count=$2,complete=complete OR $3 WHERE snapshot_id=$1`, p.SnapshotId, rows, p.NextCursor == "")
	if err != nil {
		return zero, err
	}
	if err = tx.Commit(ctx); err != nil {
		return zero, err
	}
	return Generation{SnapshotID: p.SnapshotId, C: p.BoundaryC, ExpiresAt: expiry, Count: rows, Complete: complete || p.NextCursor == ""}, nil
}

// Info is diagnostic durable state, never a readiness or activation decision.
func (l *Loader) Info(ctx context.Context, id string) (Generation, error) {
	g := Generation{SnapshotID: id}
	var c, expires string
	err := l.pool.QueryRow(ctx, `SELECT boundary_c::text,expires_text,row_count,complete FROM app.matcher_snapshot_generations WHERE snapshot_id=$1`, id).Scan(&c, &expires, &g.Count, &g.Complete)
	if err != nil {
		return g, err
	}
	g.C, err = strconv.ParseUint(c, 10, 64)
	if err != nil {
		return g, err
	}
	g.ExpiresAt, err = time.Parse(time.RFC3339Nano, expires)
	return g, err
}

// Get exposes full after-images only from completely loaded generations. The
// consumer must still hand off the pin and replay; this does not authorize work.
func (l *Loader) Get(ctx context.Context, snapshotID, subscriptionID string) (*pb.SnapshotSubscription, error) {
	g, err := l.Info(ctx, snapshotID)
	if err != nil {
		return nil, err
	}
	if !g.Complete {
		return nil, ErrIncomplete
	}
	var body []byte
	if err = l.pool.QueryRow(ctx, `SELECT body FROM app.matcher_snapshot_rows WHERE snapshot_id=$1 AND subscription_id=$2`, snapshotID, subscriptionID).Scan(&body); err != nil {
		return nil, err
	}
	r := new(pb.SnapshotSubscription)
	if err = proto.Unmarshal(body, r); err != nil {
		return nil, err
	}
	return r, nil
}
