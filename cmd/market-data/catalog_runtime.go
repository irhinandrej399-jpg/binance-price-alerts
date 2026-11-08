package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/stream"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/streamplan"
)

// servedCatalog exposes only the last applied plan. The refresh loop is its sole
// writer; RPC readers get their own slice. UpdatedAt remains the upstream fetch time.
type servedCatalog struct {
	mu       sync.RWMutex
	snapshot catalog.Snapshot
}

func (s *servedCatalog) Fetch(ctx context.Context) (catalog.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return catalog.Snapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := s.snapshot
	out.Pairs = append([]catalog.Pair(nil), out.Pairs...)
	return out, nil
}
func (s *servedCatalog) store(snapshot catalog.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = snapshot
	s.snapshot.Pairs = append([]catalog.Pair(nil), snapshot.Pairs...)
}

// Withdraw removals before draining sockets; new pairs become visible after ACKs.
func (s *servedCatalog) withdraw(next catalog.Snapshot) {
	keep := map[string]bool{}
	for _, p := range next.Pairs {
		keep[p.Symbol] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pairs := []catalog.Pair{}
	for _, p := range s.snapshot.Pairs {
		if keep[p.Symbol] {
			pairs = append(pairs, p)
		}
	}
	s.snapshot.Pairs = pairs
}

// generationSet keeps every generation this process prepared, including retired
// pairs. It is read for bounded gap persistence only after all callbacks join.
type generationSet struct {
	mu     sync.Mutex
	values map[string]uint64
}

func (g *generationSet) remember(pair string, gen uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.values[pair] = gen
}
func (g *generationSet) gaps(ctx context.Context, repo *storage.Repository) error {
	for pair, gen := range g.values {
		if err := repo.SetStatus(ctx, pair, gen, priceobserved.Unconfirmed, true); err != nil && !errors.Is(err, storage.ErrStale) {
			return err
		}
	}
	return nil
}
func prepareGroup(repo *storage.Repository, generations *generationSet) stream.PrepareGroupAttempt {
	return func(ctx context.Context, _ int, names []string) (stream.AttemptCallbacks, error) {
		callbacks := map[string]stream.AttemptCallbacks{}
		for _, name := range names {
			pair := strings.ToUpper(strings.TrimSuffix(name, "@trade"))
			var state atomic.Value
			var generation atomic.Uint64
			cb, err := prepare(repo, pair, &state, func(bool) {}, &generation)(ctx)
			if err != nil {
				return stream.AttemptCallbacks{}, err
			}
			generations.remember(pair, generation.Load())
			callbacks[pair] = cb
		}
		return stream.AttemptCallbacks{
			Handle: func(ctx context.Context, raw []byte) error {
				var msg struct {
					Pair string `json:"s"`
				}
				if err := json.Unmarshal(raw, &msg); err != nil {
					return err
				}
				cb, ok := callbacks[msg.Pair]
				if !ok {
					return errors.New("market-data: trade outside group plan")
				}
				return cb.Handle(ctx, raw)
			},
			Disconnected: func(ctx context.Context, cause error, delay time.Duration) error {
				for _, cb := range callbacks {
					if err := cb.Disconnected(ctx, cause, delay); err != nil {
						return err
					}
				}
				return nil
			},
		}, nil
	}
}

// supportedSnapshot excludes symbols outside the existing ASCII storage contract
// and normalizes duplicate stream names. Catalog and ingest always use this same validated set.
var supportedSymbol = regexp.MustCompile(`^[A-Z0-9]+USDT$`)

func supportedSnapshot(s catalog.Snapshot) (catalog.Snapshot, error) {
	seen := map[string]bool{}
	pairs := []catalog.Pair{}
	for _, p := range s.Pairs {
		if !supportedSymbol.MatchString(p.Symbol) || p.QuoteAsset != "USDT" {
			continue
		}
		name := strings.ToLower(p.Symbol)
		if !seen[name] {
			seen[name] = true
			pairs = append(pairs, p)
		}
	}
	s.Pairs = pairs
	_, err := streamplan.Build(s, streamplan.MaxStreamsPerConnection)
	return s, err
}
func runCatalog(ctx context.Context, source *catalog.Client, served *servedCatalog, r *stream.Reconciler, initial catalog.Snapshot, interval time.Duration) error {
	defer r.Close()
	apply := func(s catalog.Snapshot) error {
		served.withdraw(s)
		if err := r.Reconcile(ctx, s); err != nil {
			return err
		}
		served.store(s)
		return nil
	}
	if err := apply(initial); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s, err := source.Fetch(ctx)
			if err == nil {
				s, err = supportedSnapshot(s)
			}
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				fmt.Fprintln(os.Stderr, "market-data: catalog refresh failed; keeping last applied plan")
				continue
			}
			if err := apply(s); err != nil {
				return err
			}
		}
	}
}
