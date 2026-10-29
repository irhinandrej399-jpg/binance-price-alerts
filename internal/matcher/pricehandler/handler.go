// Package pricehandler evaluates the current live view and durably records
// canonical API outcomes before acknowledging a price. It owns no reader loop.
package pricehandler

import (
	"context"
	"errors"
	"math"
	"reflect"

	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrViewChanged = errors.New("pricehandler: view changed; sample again")
	ErrProtocol    = errors.New("pricehandler: inconsistent input or response")
)

type View interface {
	Current(string) (live.Selection, error)
}
type Images interface {
	ReadImage(context.Context, string) (projection.Image, error)
}
type Delivery interface {
	RequestDelivery(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error)
}

// Request.Key identifies one evaluation view, not just a price revision. Caller
// retains it across retries; a new subscription/view requires a new key. Event
// is nil for the first Latest or a Changed wakeup. Prefix is separately proven
// by the partition owner; a single pair's completion cannot prove that prefix.
type Request struct {
	Key      string
	Pair     string
	Event    *observed.Observed
	Position *pricework.Position
	Prefix   *pricework.Prefix
}
type Handler struct {
	work         *pricework.Store
	view         View
	images       Images
	api          Delivery
	projectionID string
}

func New(work *pricework.Store, view View, images Images, api Delivery, projectionID string) *Handler {
	return &Handler{work, view, images, api, projectionID}
}

// Handle must be serialized by the owner of a prices partition. It never calls
// Next, creates a workflow ID, retries in a busy loop, or commits broker offsets.
// Any error leaves the durable work retryable using exactly the same Request.
func (h *Handler) Handle(ctx context.Context, r Request, ack func(context.Context) error) error {
	if h.work == nil || h.view == nil || h.images == nil || h.api == nil || h.projectionID == "" || r.Key == "" || r.Pair == "" {
		return ErrProtocol
	}
	if (r.Position != nil && r.Prefix == nil && ack != nil) || (r.Position == nil && r.Prefix != nil) {
		return ErrProtocol
	}
	w, err := h.work.Get(ctx, r.Key)
	if errors.Is(err, pgx.ErrNoRows) {
		if err = h.accept(ctx, r); err != nil {
			return err
		}
		w, err = h.work.Get(ctx, r.Key)
	}
	if err != nil {
		return err
	}
	if w.Input.Quote.Pair != r.Pair || !reflect.DeepEqual(w.Input.Position, r.Position) {
		return pricework.ErrConflict
	}
	if r.Event != nil && (r.Event.Pair != r.Pair || w.Input.Quote.Revision != r.Event.Revision) {
		return pricework.ErrConflict
	}
	if w.State != "pending" {
		return h.work.Complete(ctx, r.Key, w.SupersededBy, r.Prefix, ack)
	}
	for _, e := range w.Entries {
		if e.Result != nil {
			continue
		}
		c := e.Candidate
		sel, err := h.current(r.Pair)
		if err != nil {
			// A durable workflow/terminal image can outlive the last active
			// pair connection. Recover the original command through API;
			// never create new work from an unavailable active-price view.
			if !errors.Is(err, live.ErrNotReady) {
				return err
			}
			im, readErr := h.images.ReadImage(ctx, c.SubscriptionID)
			if readErr != nil {
				return readErr
			}
			owner, readErr := im.OwnerID()
			if readErr != nil {
				return readErr
			}
			if owner != c.OwnerID {
				return ErrProtocol
			}
			_, state, _, _, readErr := attributes(im, c.SubscriptionID, r.Pair)
			if readErr != nil {
				return readErr
			}
			if state == "active" && !hasWorkflow(im) {
				return err
			}
		} else {
			newer, err := sel.Quote.NewerThan(w.Input.Quote)
			if err != nil {
				return err
			}
			if newer {
				return h.work.Complete(ctx, r.Key, &sel.Quote, r.Prefix, ack)
			}
			if sel.Quote.Revision != w.Input.Quote.Revision {
				return ErrViewChanged
			}
		}
		if c.Version > math.MaxInt64 {
			return ErrProtocol
		}
		// Always recover the original canonical command, even if cancellation or a
		// workflow has since reached the projection after an ambiguous prior RPC.
		response, rpcErr := h.api.RequestDelivery(ctx, c.OwnerID, c.SubscriptionID, int64(c.Version))
		var result pricework.Result
		if rpcErr == nil {
			if response == nil || response.SubscriptionId != c.SubscriptionID || response.SubscriptionVersion <= 0 || response.Workflow == nil || response.Workflow.TriggerId == "" || response.Workflow.Version <= 0 {
				return ErrProtocol
			}
			result = pricework.Result{Outcome: "workflow", TriggerID: response.Workflow.TriggerId, WorkflowVersion: uint64(response.Workflow.Version)}
		} else {
			switch status.Code(rpcErr) {
			case codes.NotFound:
				result.Outcome = "not_found"
			case codes.Aborted, codes.FailedPrecondition:
				// Codes do not distinguish inactive from false. Refresh both the latest
				// view and durable after-image; never infer a reason from message text.
				refreshed, viewErr := h.current(r.Pair)
				im, err := h.images.ReadImage(ctx, c.SubscriptionID)
				if err != nil {
					return err
				}
				owner, err := im.OwnerID()
				if err != nil {
					return err
				}
				if owner != c.OwnerID {
					return ErrProtocol
				}
				version, state, threshold, direction, err := attributes(im, c.SubscriptionID, r.Pair)
				if err != nil {
					return err
				}
				switch {
				case version > c.Version:
					result.Outcome = "version_changed"
				case version < c.Version:
					return ErrViewChanged
				case state != "active":
					result.Outcome = "not_active"
				case viewErr != nil:
					return viewErr
				case !price.Satisfied(*refreshed.Quote.Price, threshold, direction):
					result.Outcome = "condition_false"
				default:
					return rpcErr // projection/price may lag API; keep pending
				}
			default:
				return rpcErr
			}
		}
		if err = h.work.Record(ctx, r.Key, c.SubscriptionID, result); err != nil {
			return err
		}
	}
	if len(w.Entries) > 0 {
		// Every accepted candidate now has a durable canonical outcome; the
		// last pair connection may already have been pruned by live replay.
		return h.work.Complete(ctx, r.Key, nil, r.Prefix, ack)
	}
	// Even an empty candidate set has a durable result. Recheck before completing
	// so a newer quote is explicitly recorded instead of replaying historical work.
	sel, err := h.current(r.Pair)
	if err != nil {
		return err
	}
	newer, err := sel.Quote.NewerThan(w.Input.Quote)
	if err != nil {
		return err
	}
	if newer {
		return h.work.Complete(ctx, r.Key, &sel.Quote, r.Prefix, ack)
	}
	if sel.Quote.Revision != w.Input.Quote.Revision {
		return ErrViewChanged
	}
	return h.work.Complete(ctx, r.Key, nil, r.Prefix, ack)
}
func (h *Handler) current(pair string) (live.Selection, error) {
	sel, err := h.view.Current(pair)
	if err != nil {
		return sel, err
	}
	if err = sel.Quote.Validate(); err != nil {
		return sel, err
	}
	if sel.Quote.Pair != pair {
		return sel, ErrProtocol
	}
	return sel, nil
}
func (h *Handler) accept(ctx context.Context, r Request) error {
	sel, err := h.current(r.Pair)
	if err != nil {
		return err
	}
	q := sel.Quote
	if r.Event != nil {
		newer, e := sel.Quote.NewerThan(*r.Event)
		if e != nil {
			return e
		}
		if !newer && sel.Quote.Revision != r.Event.Revision {
			return ErrViewChanged
		}
		q = *r.Event
	}
	in := pricework.Input{Key: r.Key, ProjectionID: h.projectionID, C: sel.C, Quote: q, Position: r.Position}
	if q.Revision == sel.Quote.Revision {
		for _, c := range sel.Candidates {
			if c.Pair != r.Pair || c.Version == 0 || c.Version > math.MaxInt64 {
				return ErrProtocol
			}
			im, err := h.images.ReadImage(ctx, c.ID)
			if err != nil {
				return err
			}
			owner, err := im.OwnerID()
			if err != nil {
				return err
			}
			version, state, threshold, direction, err := attributes(im, c.ID, r.Pair)
			if err != nil {
				return err
			}
			if version != c.Version || state != "active" || threshold.Cmp(c.Threshold) != 0 || direction != c.Direction {
				return ErrViewChanged
			}
			if !price.Satisfied(*sel.Quote.Price, threshold, direction) {
				return ErrProtocol
			}
			in.Candidates = append(in.Candidates, pricework.Candidate{OwnerID: owner, SubscriptionID: c.ID, Version: c.Version})
		}
	}
	again, err := h.current(r.Pair)
	if err != nil {
		return err
	}
	if again.C != sel.C || again.Quote.Revision != sel.Quote.Revision {
		return ErrViewChanged
	}
	return h.work.Accept(ctx, in)
}
func attributes(im projection.Image, id, pair string) (uint64, string, price.Price, price.Direction, error) {
	if im.Snapshot != nil && im.Change == nil {
		s := im.Snapshot
		p, err := price.Parse(s.Threshold)
		if err != nil || s.SubscriptionId != id || s.Pair != pair || s.Version <= 0 || (s.Direction != "up" && s.Direction != "down") {
			return 0, "", p, "", ErrProtocol
		}
		return uint64(s.Version), s.State, p, price.Direction(s.Direction), nil
	}
	if im.Change != nil && im.Snapshot == nil {
		s := im.Change
		if err := s.Validate(); err != nil {
			return 0, "", price.Price{}, "", err
		}
		if s.SubscriptionID != id || s.Pair != pair {
			return 0, "", price.Price{}, "", ErrProtocol
		}
		return s.SubscriptionVersion, string(s.State), *s.Threshold, s.Direction, nil
	}
	return 0, "", price.Price{}, "", ErrProtocol
}

func hasWorkflow(im projection.Image) bool {
	return im.Snapshot != nil && im.Snapshot.Workflow != nil || im.Change != nil && im.Change.Workflow != nil
}
