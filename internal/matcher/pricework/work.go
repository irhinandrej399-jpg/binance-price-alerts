// Package pricework stores resumable price evaluation, independently of projection checkpoints.
package pricework

import (
	"errors"

	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalid  = errors.New("invalid price work")
	ErrConflict = errors.New("price work conflict")
	ErrPending  = errors.New("unfinished price candidates")
)

type Position struct {
	Topic     string
	Partition int32
	Offset    int64
}
type Candidate struct {
	OwnerID        string
	SubscriptionID string
	Version        uint64
}
type Input struct {
	Key          string
	ProjectionID string
	C            uint64
	Quote        observed.Observed
	Position     *Position
	Candidates   []Candidate
}

// Result is a durable terminal outcome of one candidate evaluation. Transient
// API/SQL/transport failures must not be stored as terminal results.
type Result struct {
	Outcome         string
	TriggerID       string
	WorkflowVersion uint64
}
type Entry struct {
	Candidate Candidate
	Result    *Result
}
type Work struct {
	Input        Input
	State        string
	SupersededBy *observed.Observed
	Completed    int
	Entries      []Entry
}

// Prefix is an explicit caller assertion that [From,Next) is a continuous,
// durably handled read_committed prefix. Physical offsets may have Kafka gaps.
type Prefix struct {
	Topic     string
	Partition int32
	From      int64
	Next      int64
}
type Store struct {
	pool  *pgxpool.Pool
	scope string
	grant *fencing.Grant
}

func New(pool *pgxpool.Pool, scope string) *Store { return &Store{pool: pool, scope: scope} }

// NewFenced binds the stable pricework scope to one durable SQL assignment.
func NewFenced(pool *pgxpool.Pool, grant fencing.Grant) *Store {
	return &Store{pool: pool, scope: grant.Scope, grant: &grant}
}
