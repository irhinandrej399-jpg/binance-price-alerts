package trade

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
)

var ErrInvalidFilterInput = errors.New("invalid trade filter input")

// Decision describes a candidate, not a durable acceptance or publication.
// GapDetected is meaningful only when Accept is true.
type Decision struct {
	Accept      bool
	GapDetected bool
}

// CheckNext compares raw trade IDs within one pair. previous must be the last
// committed observation, retained across reconnects and restored after restart.
// nil means no prior observation is known. No input or internal state is changed.
// The caller serializes this check with durable acceptance for the pair (MKT-017).
func CheckNext(previous *priceobserved.Observed, candidate priceobserved.Observed) (Decision, error) {
	next, err := filterID(candidate)
	if err != nil {
		return Decision{}, err
	}
	if previous == nil {
		return Decision{Accept: true, GapDetected: candidate.GapDetected}, nil
	}
	last, err := filterID(*previous)
	if err != nil {
		return Decision{}, err
	}
	if previous.Pair != candidate.Pair {
		return Decision{}, fmt.Errorf("%w: different pairs", ErrInvalidFilterInput)
	}
	if next <= last {
		return Decision{}, nil
	}
	// Subtract only after comparing, avoiding overflow at MaxUint64.
	return Decision{Accept: true, GapDetected: previous.GapDetected || candidate.GapDetected || next-last > 1}, nil
}

func filterID(q priceobserved.Observed) (uint64, error) {
	if err := q.Validate(); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrInvalidFilterInput, err)
	}
	if q.Revision.StreamID != StreamID {
		return 0, fmt.Errorf("%w: raw trade stream required", ErrInvalidFilterInput)
	}
	id, err := strconv.ParseUint(q.SourceID, 10, 64)
	if err != nil || strconv.FormatUint(id, 10) != q.SourceID {
		return 0, fmt.Errorf("%w: canonical uint64 source ID required", ErrInvalidFilterInput)
	}
	return id, nil
}
