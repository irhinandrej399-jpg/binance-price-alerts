// Package streamplan builds a deterministic raw-trade stream plan from a catalog.
package streamplan

import (
	"errors"
	"slices"
	"strings"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
)

const MaxStreamsPerConnection = 1024

var (
	ErrInvalidLimit  = errors.New("streamplan: limit must be between 1 and 1024")
	ErrInvalidSymbol = errors.New("streamplan: empty catalog symbol")
)

// Build returns lexicographically sorted unique <lowercase-symbol>@trade names,
// split into groups of at most limit streams. Input must be a successful catalog
// snapshot; filtering tradability and asset metadata belongs to catalog.Fetch.
// UpdatedAt is not used. Empty input returns nil; errors return no partial plan.
// Build does not modify the snapshot or open any connections.
func Build(snapshot catalog.Snapshot, limit int) ([][]string, error) {
	if limit < 1 || limit > MaxStreamsPerConnection {
		return nil, ErrInvalidLimit
	}
	unique := make(map[string]struct{}, len(snapshot.Pairs))
	for _, pair := range snapshot.Pairs {
		if pair.Symbol == "" {
			return nil, ErrInvalidSymbol
		}
		unique[strings.ToLower(pair.Symbol)+"@trade"] = struct{}{}
	}
	streams := make([]string, 0, len(unique))
	for name := range unique {
		streams = append(streams, name)
	}
	slices.Sort(streams)
	var groups [][]string
	for len(streams) > 0 {
		n := min(limit, len(streams))
		groups = append(groups, streams[:n:n])
		streams = streams[n:]
	}
	return groups, nil
}
