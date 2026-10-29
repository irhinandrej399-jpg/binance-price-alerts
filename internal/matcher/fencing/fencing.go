// Package fencing serializes durable assignment changes with matcher SQL writes.
// A grant is authority in this database, never proof of Kafka group ownership.
package fencing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrAuthority = errors.New("matcher SQL authority missing, mismatched or stale")
	ErrExhausted = errors.New("matcher fencing token exhausted")
)

type Identity struct {
	Environment string
	Group       string
	Topic       string
	Partition   int32
}
type Grant struct {
	Identity     Identity
	Scope        string
	ProjectionID string
	Token        int64
}
type Request struct {
	Identity      Identity
	Scope         string
	ProjectionID  string
	RequestID     string
	ExpectedToken int64
}
type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }
func nonempty(s string) bool        { return strings.TrimSpace(s) != "" }
func key(i Identity) string         { b, _ := json.Marshal(i); return string(b) }
func valid(g Grant) bool {
	i := g.Identity
	return nonempty(i.Environment) && nonempty(i.Group) && nonempty(i.Topic) && i.Partition >= 0 && nonempty(g.Scope) && nonempty(g.ProjectionID) && g.Token > 0
}

// Lock must precede all other application row locks in a mutating transaction.
// The SHARE lock is retained through commit/rollback, including legacy writes.
func Lock(ctx context.Context, tx pgx.Tx) error {
	var id int
	return tx.QueryRow(ctx, `SELECT id FROM app.matcher_fence_gate WHERE id=1 FOR SHARE`).Scan(&id)
}
func exclusive(ctx context.Context, tx pgx.Tx) error {
	var id int
	return tx.QueryRow(ctx, `SELECT id FROM app.matcher_fence_gate WHERE id=1 FOR UPDATE`).Scan(&id)
}

// Assign is a CAS on the previous token. RequestID must be stable before the
// first call. Repeating the exact request recovers its original grant, even if
// subsequently superseded/revoked; the caller must still validate before writes.
func (s *Store) Assign(ctx context.Context, r Request) (Grant, error) {
	g := Grant{Identity: r.Identity, Scope: r.Scope, ProjectionID: r.ProjectionID, Token: 1}
	if !valid(g) || !nonempty(r.RequestID) || r.ExpectedToken < 0 {
		return Grant{}, ErrAuthority
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Grant{}, err
	}
	defer tx.Rollback(context.Background())
	if err = exclusive(ctx, tx); err != nil {
		return Grant{}, err
	}
	body, _ := json.Marshal(r)
	var oldBody, oldGrant []byte
	err = tx.QueryRow(ctx, `SELECT request_body,grant_body FROM app.matcher_assignment_requests WHERE assignment_key=$1 AND request_id=$2`, key(r.Identity), r.RequestID).Scan(&oldBody, &oldGrant)
	if err == nil {
		if !bytes.Equal(body, oldBody) {
			return Grant{}, ErrAuthority
		}
		if err = json.Unmarshal(oldGrant, &g); err != nil {
			return Grant{}, err
		}
		return g, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Grant{}, err
	}
	var token int64
	var scope string
	err = tx.QueryRow(ctx, `SELECT token,scope FROM app.matcher_assignments WHERE assignment_key=$1`, key(r.Identity)).Scan(&token, &scope)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Grant{}, err
	}
	if token != r.ExpectedToken || (token != 0 && scope != r.Scope) {
		return Grant{}, ErrAuthority
	}
	// One stable scope belongs to exactly one fixed partition identity forever.
	var occupied bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.matcher_assignments WHERE scope=$1 AND assignment_key<>$2)`, r.Scope, key(r.Identity)).Scan(&occupied); err != nil {
		return Grant{}, err
	}
	if occupied {
		return Grant{}, ErrAuthority
	}
	if token == math.MaxInt64 {
		return Grant{}, ErrExhausted
	}
	g.Token = token + 1
	gb, _ := json.Marshal(g)
	if _, err = tx.Exec(ctx, `INSERT INTO app.matcher_assignments(assignment_key,scope,token,grant_body,active) VALUES($1,$2,$3,$4,true) ON CONFLICT(assignment_key) DO UPDATE SET token=EXCLUDED.token,grant_body=EXCLUDED.grant_body,active=true`, key(r.Identity), r.Scope, g.Token, gb); err != nil {
		return Grant{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO app.matcher_projection_grants(projection_id,assignment_key,grant_body) VALUES($1,$2,$3) ON CONFLICT(projection_id,assignment_key) DO UPDATE SET grant_body=EXCLUDED.grant_body`, r.ProjectionID, key(r.Identity), gb); err != nil {
		return Grant{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO app.matcher_assignment_requests(assignment_key,request_id,request_body,grant_body) VALUES($1,$2,$3,$4)`, key(r.Identity), r.RequestID, body, gb); err != nil {
		return Grant{}, err
	}
	return g, tx.Commit(ctx)
}
func checkGrant(ctx context.Context, tx pgx.Tx, g Grant, requireActive bool) error {
	if !valid(g) {
		return ErrAuthority
	}
	var body []byte
	var active bool
	err := tx.QueryRow(ctx, `SELECT grant_body,active FROM app.matcher_assignments WHERE assignment_key=$1`, key(g.Identity)).Scan(&body, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrAuthority
	}
	if err != nil {
		return err
	}
	var current Grant
	if err = json.Unmarshal(body, &current); err != nil {
		return err
	}
	if current != g || (requireActive && !active) {
		return ErrAuthority
	}
	return nil
}

// Revoke is conditional and idempotent. A stale grant cannot revoke a successor.
func (s *Store) Revoke(ctx context.Context, g Grant) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = exclusive(ctx, tx); err != nil {
		return err
	}
	if err = checkGrant(ctx, tx, g, false); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE app.matcher_assignments SET active=false WHERE assignment_key=$1`, key(g.Identity)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Price checks a scope after Lock, in the same transaction as the mutation.
// nil preserves isolated legacy scopes, but can never write an enrolled scope.
func Price(ctx context.Context, tx pgx.Tx, scope string, g *Grant) error {
	var protected bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.matcher_assignments WHERE scope=$1)`, scope).Scan(&protected); err != nil {
		return err
	}
	if g == nil {
		if protected {
			return ErrAuthority
		}
		return nil
	}
	if !protected || scope != g.Scope {
		return ErrAuthority
	}
	return checkGrant(ctx, tx, *g, true)
}

// Projection requires the exact full membership of a replica-local generation.
// Losing any required partition fences the whole C/after-image writer.
func Projection(ctx context.Context, tx pgx.Tx, id string, grants []Grant) error {
	rows, err := tx.Query(ctx, `SELECT grant_body FROM app.matcher_projection_grants WHERE projection_id=$1 ORDER BY assignment_key`, id)
	if err != nil {
		return err
	}
	var required []Grant
	for rows.Next() {
		var body []byte
		if err = rows.Scan(&body); err != nil {
			rows.Close()
			return err
		}
		var g Grant
		if err = json.Unmarshal(body, &g); err != nil {
			rows.Close()
			return err
		}
		required = append(required, g)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(required) != len(grants) {
		return ErrAuthority
	}
	supplied := make(map[string]Grant, len(grants))
	for _, g := range grants {
		if g.ProjectionID != id {
			return ErrAuthority
		}
		k := key(g.Identity)
		if _, ok := supplied[k]; ok {
			return ErrAuthority
		}
		supplied[k] = g
	}
	for _, g := range required {
		if supplied[key(g.Identity)] != g {
			return ErrAuthority
		}
		if err = checkGrant(ctx, tx, g, true); err != nil {
			return err
		}
	}
	return nil
}

// Current observes the last durable grant, including its active flag, to build
// an ExpectedToken CAS. Absence is pgx.ErrNoRows (first ExpectedToken is zero).
// The observation alone never authorizes a subsequent mutation.
func (s *Store) Current(ctx context.Context, identity Identity) (Grant, bool, error) {
	var g Grant
	var active bool
	var body []byte
	err := s.pool.QueryRow(ctx, `SELECT grant_body,active FROM app.matcher_assignments WHERE assignment_key=$1`, key(identity)).Scan(&body, &active)
	if err != nil {
		return g, false, err
	}
	err = json.Unmarshal(body, &g)
	return g, active, err
}
