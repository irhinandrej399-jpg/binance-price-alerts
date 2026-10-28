package subscription

import (
	"context"
	"net/http"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/jackc/pgx/v5"
)

// ListDB is implemented by pgxpool.Pool, pgx.Conn and pgx.Tx.
type ListDB interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type listedWorkflow struct {
	TriggerID string `json:"trigger_id"`
	Version   int64  `json:"version"`
}
type listedBlock struct {
	Reason         domain.BlockReason `json:"reason"`
	BindingVersion int64              `json:"binding_version"`
}
type listItem struct {
	ID        string          `json:"id"`
	Pair      string          `json:"pair"`
	Threshold price.Price     `json:"threshold"`
	Direction price.Direction `json:"direction"`
	Percent   *string         `json:"percent,omitempty"`
	State     domain.State    `json:"state"`
	Version   int64           `json:"version"`
	Workflow  *listedWorkflow `json:"workflow,omitempty"`
	Blocked   *listedBlock    `json:"blocked,omitempty"`
}

// NewListHandler mounts GET /v1/subscriptions. Wrap with auth.New. Reads never
// transition state or consult Telegram/market-data. Mount alongside NewHandler
// at the parent router; runtime wiring belongs to a separate ticket.
func NewListHandler(db ListDB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		owner, ok := auth.UserID(r.Context())
		if !ok {
			createError(w, 401, "invalid_session")
			return
		}
		// One statement gives a consistent view and uses the existing (owner_id,id)
		// index. State, rather than the internal quota bookkeeping flag, is canonical.
		rows, err := db.Query(r.Context(), `SELECT `+recordColumns+` FROM app.subscriptions
   WHERE owner_id=$1 AND state IN ('active','pending_delivery','sending','delivery_blocked')
   ORDER BY id ASC LIMIT 15`, owner)
		if err != nil {
			createError(w, 500, "internal_error")
			return
		}
		defer rows.Close()
		items := make([]listItem, 0, 15)
		for rows.Next() {
			v, err := scanRecord(rows)
			if err != nil {
				createError(w, 500, "internal_error")
				return
			}
			item := listItem{ID: v.ID, Pair: v.Pair, Threshold: v.Threshold, Direction: v.Direction,
				Percent: v.Percent, State: v.State, Version: v.Version}
			if v.Workflow != nil {
				item.Workflow = &listedWorkflow{TriggerID: v.Workflow.TriggerID, Version: v.Workflow.Version}
			}
			if v.Blocked != nil {
				item.Blocked = &listedBlock{Reason: v.Blocked.Reason, BindingVersion: v.Blocked.BindingVersion}
			}
			items = append(items, item)
		}
		// Buffer the bounded result: a late row/connection failure cannot yield a
		// successful partial list. Do not expose raw database errors.
		if rows.Err() != nil || r.Context().Err() != nil {
			createError(w, 500, "internal_error")
			return
		}
		createJSON(w, 200, struct {
			Subscriptions []listItem `json:"subscriptions"`
		}{items})
	})
	return mux
}
