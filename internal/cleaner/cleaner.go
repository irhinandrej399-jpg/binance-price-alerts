package cleaner

import (
	"context"
	"errors"
	"os"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
)

type Broker interface {
	Bounds(context.Context, kafkasource.Identity, int32) (start, lso int64, err error)
	Delete(context.Context, kafkasource.Identity, int32, int64) (lowWatermark int64, err error)
}

// Reader must return exact source-bound SQL proof; broker committed offsets are
// deliberately absent from this interface. RPCReader supplies the real adapter.
type Reader interface {
	Next(context.Context, Consumer, kafkasource.Identity, int32) (int64, error)
}
type Engine struct {
	store  *Store
	broker Broker
	reader Reader
}

func New(s *Store, b Broker, r Reader) *Engine { return &Engine{s, b, r} }
func (e *Engine) valid() bool {
	return e != nil && e.store != nil && e.broker != nil && e.reader != nil
}
func (e *Engine) bounds(ctx context.Context, s State) (int64, int64, error) {
	start, end, err := e.broker.Bounds(ctx, s.Source, s.Partition)
	if err != nil || start < 0 || end < start || s.Floor > end {
		return 0, 0, errors.Join(ErrBroker, ctx.Err())
	}
	return start, end, nil
}
func (e *Engine) Init(ctx context.Context, in Inventory) error {
	s := State{Version: 1, Inventory: in}
	if !e.valid() || !s.valid() {
		return ErrConfig
	}
	for _, c := range in.Consumers {
		if c.Retired {
			return ErrConfig
		}
	}
	return e.store.locked(ctx, func(ctx context.Context) error {
		if _, err := os.Lstat(e.store.path()); !os.IsNotExist(err) {
			return ErrState
		}
		start, _, err := e.bounds(ctx, s)
		if err != nil {
			return err
		}
		s.Floor = start
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return e.store.save(s)
	})
}
func (e *Engine) Register(ctx context.Context, c Consumer) error {
	if !e.valid() || !c.valid() || c.Retired {
		return ErrConfig
	}
	return e.store.locked(ctx, func(ctx context.Context) error {
		s, err := e.store.load()
		if err != nil {
			return err
		}
		for _, old := range s.Consumers {
			if old.Name == c.Name || old.identity() == c.identity() {
				if old == c {
					return e.store.save(s)
				}
				return ErrState
			}
		}
		start, end, err := e.bounds(ctx, s)
		if err != nil {
			return err
		}
		floor := max(start, s.Floor)
		next, err := e.reader.Next(ctx, c, s.Source, s.Partition)
		if err != nil || next < floor || next > end {
			return errors.Join(ErrProof, ctx.Err())
		}
		// Recheck the source/range after the RPC. Administrative topic changes are
		// excluded operationally; an in-flight former intent may have raised start.
		start, end, err = e.bounds(ctx, s)
		if err != nil {
			return err
		}
		floor = max(start, s.Floor)
		if next < floor || next > end {
			return ErrProof
		}
		s.Floor = floor
		s.Consumers = append(s.Consumers, c)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return e.store.save(s)
	})
}

// Retire is an explicit operator decision to abandon this exact obligation.
// Tombstones prevent accidental reuse of a retired name or SQL generation.
func (e *Engine) Retire(ctx context.Context, c Consumer) error {
	if !e.valid() || !c.valid() || c.Retired {
		return ErrConfig
	}
	return e.store.locked(ctx, func(ctx context.Context) error {
		s, err := e.store.load()
		if err != nil {
			return err
		}
		for i, old := range s.Consumers {
			if old.Name != c.Name {
				continue
			}
			retired := old.Retired
			old.Retired = false
			if old != c {
				return ErrState
			}
			if retired {
				return e.store.save(s)
			}
			s.Consumers[i].Retired = true
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return e.store.save(s)
		}
		return ErrState
	})
}

type Result struct {
	Status       string `json:"status"`
	Start        int64  `json:"start"`
	Minimum      int64  `json:"minimum"`
	Target       int64  `json:"target"`
	Floor        int64  `json:"floor"`
	LowWatermark *int64 `json:"low_watermark,omitempty"`
	Attempted    bool   `json:"attempted"`
	BrokerCode   *int16 `json:"broker_code,omitempty"`
}

// Clean handles one partition and at most limit numeric offsets (not bytes).
// Any error before reservation prohibits DeleteRecords; any error afterwards
// retains the reservation because the broker may still execute the request.
func (e *Engine) Clean(ctx context.Context, limit int64) (Result, error) {
	result := Result{Status: "blocked"}
	if !e.valid() || limit < 1 || limit > 1_000_000 {
		return result, ErrConfig
	}
	err := e.store.locked(ctx, func(ctx context.Context) error {
		s, err := e.store.load()
		if err != nil {
			return err
		}
		result.Floor = s.Floor
		start, end, err := e.bounds(ctx, s)
		if err != nil {
			return err
		}
		nexts := []int64{}
		minimum := end
		for _, c := range s.Consumers {
			if c.Retired {
				continue
			}
			n, err := e.reader.Next(ctx, c, s.Source, s.Partition)
			if err != nil || n < max(start, s.Floor) || n > end {
				return errors.Join(ErrProof, ctx.Err())
			}
			nexts = append(nexts, n)
			minimum = min(minimum, n)
		}
		if len(nexts) == 0 {
			return ErrProof
		}
		start, end, err = e.bounds(ctx, s)
		if err != nil {
			return err
		}
		for _, n := range nexts {
			if n < max(start, s.Floor) || n > end {
				return ErrProof
			}
		}
		result.Start = start
		result.Minimum = minimum
		// Avoid overflow near MaxInt64; target remains <= end and <= minimum.
		target := minimum
		if target-start > limit {
			target = start + limit
		}
		result.Target = target
		if target <= start {
			result.Status = "noop"
			return nil
		}
		// Always sync the whole state again, even for a repeated target. A
		// previous replace could have succeeded while directory fsync failed.
		s.Floor = max(s.Floor, target)
		if err = e.store.save(s); err != nil {
			return err
		}
		result.Floor = s.Floor
		if ctx.Err() != nil {
			return ctx.Err()
		}
		result.Attempted = true
		result.Status = "unknown"
		low, err := e.broker.Delete(ctx, s.Source, s.Partition, target)
		if err != nil {
			var rejected *BrokerRejection
			if errors.As(err, &rejected) {
				code := rejected.Code
				result.BrokerCode = &code
				result.Status = "rejected"
			}
			return errors.Join(ErrDelete, ctx.Err())
		}
		if low < target || low > s.Floor {
			return errors.Join(ErrDelete, ctx.Err())
		}
		result.LowWatermark = &low
		code := int16(0)
		result.BrokerCode = &code
		result.Status = "deleted"
		return nil
	})
	return result, err
}
