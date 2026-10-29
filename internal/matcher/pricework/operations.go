package pricework

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/jackc/pgx/v5"
)

func (s *Store) valid(key string) bool {
	return strings.TrimSpace(s.scope) != "" && strings.TrimSpace(key) != ""
}

// canonicalInput freezes the exact view and candidate set for retries. Candidate
// order is irrelevant; one subscription appears at most once in a work item.
func canonicalInput(in Input) ([]byte, error) {
	if strings.TrimSpace(in.Key) == "" || strings.TrimSpace(in.ProjectionID) == "" {
		return nil, ErrInvalid
	}
	if err := in.Quote.Validate(); err != nil {
		return nil, err
	}
	if p := in.Position; p != nil && (strings.TrimSpace(p.Topic) == "" || p.Partition < 0 || p.Offset < 0 || p.Offset == int64(^uint64(0)>>1)) {
		return nil, ErrInvalid
	}
	in.Quote.TradeTime = in.Quote.TradeTime.UTC()
	in.Quote.ReceivedAt = in.Quote.ReceivedAt.UTC()
	if in.Quote.SourceEventTime != nil {
		tm := in.Quote.SourceEventTime.UTC()
		in.Quote.SourceEventTime = &tm
	}
	in.Candidates = append([]Candidate{}, in.Candidates...)
	sort.Slice(in.Candidates, func(i, j int) bool { return in.Candidates[i].SubscriptionID < in.Candidates[j].SubscriptionID })
	for i, c := range in.Candidates {
		if strings.TrimSpace(c.OwnerID) == "" || strings.TrimSpace(c.SubscriptionID) == "" || c.Version == 0 || (i > 0 && in.Candidates[i-1].SubscriptionID == c.SubscriptionID) {
			return nil, ErrInvalid
		}
	}
	return json.Marshal(in)
}

// Accept atomically persists the immutable price/view and all candidates. Key
// must be stable across retries and distinct for a new evaluation view.
func (s *Store) Accept(ctx context.Context, in Input) error {
	if !s.valid(in.Key) {
		return ErrInvalid
	}
	body, err := canonicalInput(in)
	if err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = fencing.Lock(ctx, tx); err != nil {
		return err
	}
	if err = fencing.Price(ctx, tx, s.scope, s.grant); err != nil {
		return err
	}

	if err = s.authorizeInput(ctx, tx, in, false); err != nil {
		// Exact acceptance retry may refer to an older generation, whose identity
		// remains immutable audit data. Never insert new work from an old view.
		if s.grant == nil || !errors.Is(err, fencing.ErrAuthority) {
			return err
		}
		var old []byte
		if e := tx.QueryRow(ctx, `SELECT input FROM app.matcher_price_work WHERE scope=$1 AND work_key=$2`, s.scope, in.Key).Scan(&old); e != nil || !bytes.Equal(old, body) {
			return err
		}
		if err = s.authorizeInput(ctx, tx, in, true); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	tag, err := tx.Exec(ctx, `INSERT INTO app.matcher_price_work(scope,work_key,input,total) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, s.scope, in.Key, body, len(in.Candidates))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var old []byte
		if err = tx.QueryRow(ctx, `SELECT input FROM app.matcher_price_work WHERE scope=$1 AND work_key=$2`, s.scope, in.Key).Scan(&old); err != nil {
			return err
		}
		if !bytes.Equal(old, body) {
			return ErrConflict
		}
	} else {
		for _, c := range in.Candidates {
			b, err := json.Marshal(c)
			if err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `INSERT INTO app.matcher_price_candidates(scope,work_key,subscription_id,candidate) VALUES($1,$2,$3,$4)`, s.scope, in.Key, c.SubscriptionID, b); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

// Get reads work and all candidate outcomes from one consistent SQL snapshot.
// Missing keys return pgx.ErrNoRows. It never creates or resets state.
func (s *Store) Get(ctx context.Context, key string) (Work, error) {
	var w Work
	if !s.valid(key) {
		return w, ErrInvalid
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return w, err
	}
	defer tx.Rollback(context.Background())
	var body, replacement []byte
	err = tx.QueryRow(ctx, `SELECT input,state,superseded_by,completed FROM app.matcher_price_work WHERE scope=$1 AND work_key=$2`, s.scope, key).Scan(&body, &w.State, &replacement, &w.Completed)
	if err != nil {
		return w, err
	}
	if err = json.Unmarshal(body, &w.Input); err != nil {
		return w, err
	}
	if replacement != nil {
		w.SupersededBy = new(observed.Observed)
		if err = json.Unmarshal(replacement, w.SupersededBy); err != nil {
			return w, err
		}
	}
	rows, err := tx.Query(ctx, `SELECT candidate,result FROM app.matcher_price_candidates WHERE scope=$1 AND work_key=$2 ORDER BY subscription_id`, s.scope, key)
	if err != nil {
		return w, err
	}
	defer rows.Close()
	for rows.Next() {
		var b, r []byte
		var e Entry
		if err = rows.Scan(&b, &r); err != nil {
			return w, err
		}
		if err = json.Unmarshal(b, &e.Candidate); err != nil {
			return w, err
		}
		if r != nil {
			e.Result = new(Result)
			if err = json.Unmarshal(r, e.Result); err != nil {
				return w, err
			}
		}
		w.Entries = append(w.Entries, e)
	}
	if err = rows.Err(); err != nil {
		return w, err
	}
	return w, tx.Commit(ctx)
}

// Pending enumerates durable work identities for restart, including empty work
// awaiting completion. Superseded work retains its outcomes but is not pending.
func (s *Store) Pending(ctx context.Context) ([]string, error) {
	if strings.TrimSpace(s.scope) == "" {
		return nil, ErrInvalid
	}
	rows, err := s.pool.Query(ctx, `SELECT work_key FROM app.matcher_price_work WHERE scope=$1 AND state='pending' ORDER BY work_key`, s.scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := []string{}
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}
func validResult(r Result) bool {
	switch r.Outcome {
	case "workflow":
		return strings.TrimSpace(r.TriggerID) != "" && r.WorkflowVersion > 0
	case "condition_false", "version_changed", "not_active", "not_found":
		return r.TriggerID == "" && r.WorkflowVersion == 0
	}
	return false
}

// Record commits one terminal outcome and increments local progress atomically.
// Same result retries succeed, even after completion; conflicting results fail.
func (s *Store) Record(ctx context.Context, key, subscriptionID string, result Result) error {
	if !s.valid(key) || !validResult(result) {
		return ErrInvalid
	}
	body, err := json.Marshal(result)
	if err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = fencing.Lock(ctx, tx); err != nil {
		return err
	}
	if err = fencing.Price(ctx, tx, s.scope, s.grant); err != nil {
		return err
	}

	var state string
	var inputBody []byte
	if err = tx.QueryRow(ctx, `SELECT state,input FROM app.matcher_price_work WHERE scope=$1 AND work_key=$2 FOR UPDATE`, s.scope, key).Scan(&state, &inputBody); err != nil {
		return err
	}
	var in Input
	if err = json.Unmarshal(inputBody, &in); err != nil {
		return err
	}
	if err = s.authorizeInput(ctx, tx, in, true); err != nil {
		return err
	}

	var old []byte
	if err = tx.QueryRow(ctx, `SELECT result FROM app.matcher_price_candidates WHERE scope=$1 AND work_key=$2 AND subscription_id=$3`, s.scope, key, subscriptionID).Scan(&old); err != nil {
		return err
	}
	if old != nil {
		if !bytes.Equal(body, old) {
			return ErrConflict
		}
		return tx.Commit(ctx)
	}
	if state != "pending" {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE app.matcher_price_candidates SET result=$4 WHERE scope=$1 AND work_key=$2 AND subscription_id=$3`, s.scope, key, subscriptionID, body); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE app.matcher_price_work SET completed=completed+1 WHERE scope=$1 AND work_key=$2`, s.scope, key); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Complete commits normal completion or explicit supersession and, optionally,
// a caller-proven continuous prices prefix. Ack runs only after successful commit.
// Any error (including commit uncertainty) requires Get/Checkpoint or retry.
func (s *Store) Complete(ctx context.Context, key string, replacement *observed.Observed, prefix *Prefix, ack func(context.Context) error) error {
	if !s.valid(key) {
		return ErrInvalid
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = fencing.Lock(ctx, tx); err != nil {
		return err
	}
	if err = fencing.Price(ctx, tx, s.scope, s.grant); err != nil {
		return err
	}

	var body, oldReplacement []byte
	var state string
	var total, completed int
	err = tx.QueryRow(ctx, `SELECT input,state,superseded_by,total,completed FROM app.matcher_price_work WHERE scope=$1 AND work_key=$2 FOR UPDATE`, s.scope, key).Scan(&body, &state, &oldReplacement, &total, &completed)
	if err != nil {
		return err
	}
	var in Input
	if err = json.Unmarshal(body, &in); err != nil {
		return err
	}
	if err = s.authorizeInput(ctx, tx, in, true); err != nil {
		return err
	}

	target := "completed"
	var nextReplacement []byte
	if replacement != nil {
		newer, e := replacement.NewerThan(in.Quote)
		if e != nil || !newer {
			return ErrInvalid
		}
		nextReplacement, err = json.Marshal(replacement)
		if err != nil {
			return err
		}
		target = "superseded"
	}
	if state != "pending" {
		if state != target || !bytes.Equal(oldReplacement, nextReplacement) {
			return ErrConflict
		}
	} else {
		if replacement == nil && completed != total {
			return ErrPending
		}
		if _, err = tx.Exec(ctx, `UPDATE app.matcher_price_work SET state=$3,superseded_by=$4 WHERE scope=$1 AND work_key=$2`, s.scope, key, target, nextReplacement); err != nil {
			return err
		}
	}
	if prefix != nil {
		p := in.Position
		if p == nil || prefix.Topic != p.Topic || prefix.Partition != p.Partition || prefix.From < 0 || prefix.From > p.Offset || prefix.Next <= p.Offset {
			return ErrInvalid
		}
		if _, err = tx.Exec(ctx, `INSERT INTO app.matcher_price_checkpoints(scope,topic,partition,next_offset) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, s.scope, prefix.Topic, prefix.Partition, prefix.From); err != nil {
			return err
		}
		var current int64
		if err = tx.QueryRow(ctx, `SELECT next_offset FROM app.matcher_price_checkpoints WHERE scope=$1 AND topic=$2 AND partition=$3 FOR UPDATE`, s.scope, prefix.Topic, prefix.Partition).Scan(&current); err != nil {
			return err
		}
		if current < prefix.Next {
			if current != prefix.From {
				return ErrConflict
			}
			if _, err = tx.Exec(ctx, `UPDATE app.matcher_price_checkpoints SET next_offset=$4 WHERE scope=$1 AND topic=$2 AND partition=$3`, s.scope, prefix.Topic, prefix.Partition, prefix.Next); err != nil {
				return err
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if ack != nil {
		if err = ack(ctx); err != nil {
			return fmt.Errorf("ack durable price work: %w", err)
		}
	}
	return nil
}

// Checkpoint returns the independent prices transport prefix, or pgx.ErrNoRows
// before an explicitly asserted prefix has committed. No offset is inferred.
func (s *Store) Checkpoint(ctx context.Context, topic string, partition int32) (int64, error) {
	if strings.TrimSpace(s.scope) == "" || strings.TrimSpace(topic) == "" || partition < 0 {
		return 0, ErrInvalid
	}
	var next int64
	err := s.pool.QueryRow(ctx, `SELECT next_offset FROM app.matcher_price_checkpoints WHERE scope=$1 AND topic=$2 AND partition=$3`, s.scope, topic, partition).Scan(&next)
	return next, err
}

// Resuming existing work keeps its original projection as audit. Only new work
// must use the assignment's current generation; transport identity stays fixed.
func (s *Store) authorizeInput(ctx context.Context, tx pgx.Tx, in Input, resume bool) error {
	if s.grant == nil {
		return fencing.Projection(ctx, tx, in.ProjectionID, nil)
	}
	if !resume && in.ProjectionID != s.grant.ProjectionID {
		return fencing.ErrAuthority
	}
	if p := in.Position; p != nil && (p.Topic != s.grant.Identity.Topic || p.Partition != s.grant.Identity.Partition) {
		return fencing.ErrAuthority
	}
	return nil
}
