package changefeed

import (
	"errors"
	"testing"
	"time"
)

func TestIntegrationResumeUsesSQLPosition(t *testing.T) {
	cfg, p := integration(t, 0)
	first := source(t, cfg)
	r := registration(t, first)
	publish(t, p, cfg, true, record(0, 1, 1))
	tx := next(t, r)
	// SQL committed, but Confirm/broker commit was lost. Repeat the same whole batch.
	start := r.Start()
	first.Close()
	second := source(t, cfg)
	resumed, err := second.RegisterAt(bounded(t, 25*time.Second), start)
	if err != nil {
		t.Fatal(err)
	}
	repeated := next(t, resumed)
	if repeated.C != tx.C || repeated.NextOffset != tx.NextOffset {
		t.Fatal(repeated, tx)
	}
	ack(t, resumed, repeated)
	second.Close()
	// Once SQL records the next offset, restart begins there even with no broker commit.
	publish(t, p, cfg, true, record(0, 2, 2))
	start.Offset = tx.NextOffset
	third := source(t, cfg)
	resumed, err = third.RegisterAt(bounded(t, 25*time.Second), start)
	if err != nil {
		t.Fatal(err)
	}
	if got := next(t, resumed); got.First != 2 {
		t.Fatal(got)
	}
	for _, pos := range []Position{{Topic: cfg.Topic + "other"}, {Topic: cfg.Topic, Partition: 1}, {Topic: cfg.Topic, Offset: -1}, {Topic: cfg.Topic, Offset: 1 << 60}} {
		s := source(t, cfg)
		if _, err = s.RegisterAt(bounded(t, 5*time.Second), pos); !errors.Is(err, ErrPosition) {
			t.Fatal(pos, err)
		}
	}
}
