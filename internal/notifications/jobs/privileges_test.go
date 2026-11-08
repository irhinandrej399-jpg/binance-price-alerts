package jobs

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestRuntimeEvidencePrivileges(t *testing.T) {
	ctx, p, _, _, _, _ := fixture(t)
	var role string
	if err := p.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil || role != "notifications_runtime" {
		t.Fatalf("expected notifications_runtime, got %q: %v", role, err)
	}
	for _, table := range []struct {
		name, key string
		report    bool
	}{
		{"delivery_attempts", "attempt_id", false},
		{"delivery_successes", "attempt_id", false},
		{"delivery_uncertainties", "attempt_id", false},
		{"delivery_blocks", "attempt_id", false},
		{"delivery_block_parks", "claim_id", false},
		{"delivery_retryables", "attempt_id", false},
		{"delivery_success_reports", "attempt_id", true},
		{"delivery_uncertain_reports", "attempt_id", true},
		{"delivery_blocked_reports", "attempt_id", true},
		{"delivery_retryable_reports", "attempt_id", true},
	} {
		t.Run(table.name, func(t *testing.T) {
			for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"} {
				want := privilege == "SELECT" || privilege == "INSERT" || (table.report && privilege == "UPDATE")
				var got bool
				if err := p.QueryRow(ctx, `SELECT has_table_privilege(current_user,$1,$2)`, "app."+table.name, privilege).Scan(&got); err != nil {
					t.Fatal(err)
				}
				if got != want {
					t.Errorf("%s privilege = %v, want %v", privilege, got, want)
				}
			}
			// Empty predicates exercise authorization without touching shared fixture data,
			// including on the unfixed schema during Red. Existing jobs tests exercise
			// actual INSERT/SELECT and persisted acknowledgements for every outcome.
			queries := []string{"DELETE FROM app." + table.name + " WHERE false"}
			if !table.report {
				queries = append(queries, "UPDATE app."+table.name+" SET "+table.key+"="+table.key+" WHERE false")
			} else if _, err := p.Exec(ctx, "UPDATE app."+table.name+" SET pending=false WHERE false"); err != nil {
				t.Fatalf("ack permission: %v", err)
			}
			for _, query := range queries {
				_, err := p.Exec(ctx, query)
				var denied *pgconn.PgError
				if !errors.As(err, &denied) || denied.Code != "42501" {
					t.Errorf("expected insufficient_privilege for %s, got %v", query, err)
				}
			}
		})
	}
}
