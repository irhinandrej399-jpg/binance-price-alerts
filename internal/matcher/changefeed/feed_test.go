package changefeed

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

func fixture(pos, end uint64) []byte {
	return fmt.Appendf(nil, `{"event_id":"event-%d","type":"subscription.changed","version":1,"occurred_at":"2026-10-29T10:00:00Z","correlation_id":"request-1","ordering_key":"sub-1","payload":{"schema_version":1,"position":%d,"transaction_end":%d,"subscription_id":"sub-1","subscription_version":1,"state":"active","pair":"BTCUSDT","threshold":"123.456","direction":"up","activation":"latest_price","creation_snapshot":{"price":"100","trade_time":"2026-10-29T10:00:00Z"}}}`, pos, pos, end)
}
func record(offset int64, pos, end uint64) *kgo.Record {
	return &kgo.Record{Topic: "changes", Partition: 0, Offset: offset, Key: []byte("sub-1"), Value: fixture(pos, end), ProducerID: 7, ProducerEpoch: 1, Attrs: kgo.NewRecordAttrs(kgo.RecordAttrsOpts{Transactional: true})}
}
func marker(offset int64) *kgo.Record {
	r := record(offset, 1, 1)
	r.Key = []byte{0, 0, 0, 1}
	r.Value = []byte{0, 0, 0, 0, 0, 0}
	r.Attrs = kgo.NewRecordAttrs(kgo.RecordAttrsOpts{Transactional: true, Control: true})
	return r
}
func local(limit int) *Registration {
	ctx, cancel := context.WithCancel(context.Background())
	return &Registration{topic: "changes", limit: limit, ctx: ctx, cancel: cancel, changed: make(chan struct{}), done: make(chan struct{}), start: Position{"changes", 0, 10}, confirmed: 10}
}
func TestWholeTransactionAndExplicitConfirmation(t *testing.T) {
	r := local(4)
	defer r.Close()
	if !r.accept(record(10, 41, 42)) {
		t.Fatal(r.Err())
	}
	if _, ok, err := r.TryNext(); ok || err != nil {
		t.Fatal("partial package escaped", ok, err)
	}
	r.accept(record(11, 42, 42))
	if _, ok, _ := r.TryNext(); ok {
		t.Fatal("escaped before commit marker")
	}
	r.accept(marker(12))
	tx, ok, err := r.TryNext()
	if !ok || err != nil || tx.First != 41 || tx.C != 42 || tx.NextOffset != 13 || len(tx.Records) != 2 {
		t.Fatalf("%+v %v %v", tx, ok, err)
	}
	if string(tx.Records[0].JSON) != string(fixture(41, 42)) || tx.Records[0].Envelope.EventID != "event-41" || tx.Records[0].Position.Offset != 10 {
		t.Fatal(tx)
	}
	if r.Confirmed() != 10 {
		t.Fatal("automatic ack")
	}
	if err := r.Confirm(12); !errors.Is(err, ErrConfirmation) {
		t.Fatal(err)
	}
	r.accept(record(13, 49, 49))
	r.accept(marker(14)) // journal gap is visible, not hidden
	if _, ok, _ := r.TryNext(); ok {
		t.Fatal("unconfirmed prefix bypassed")
	}
	if err := r.Confirm(13); err != nil {
		t.Fatal(err)
	}
	tx, ok, err = r.TryNext()
	if !ok || err != nil || tx.First != 49 {
		t.Fatal(tx, ok, err)
	}
	if err := r.Confirm(15); err != nil {
		t.Fatal(err)
	}
	// Whole duplicate remains visible for durable deduplication.
	r.accept(record(15, 49, 49))
	r.accept(marker(16))
	tx, ok, err = r.TryNext()
	if !ok || err != nil || tx.C != 49 {
		t.Fatal(tx, ok, err)
	}
}
func TestMalformedBoundariesAndLimitsInvalidate(t *testing.T) {
	tests := map[string]func(*Registration){
		"key":      func(r *Registration) { v := record(10, 1, 1); v.Key = []byte("other"); r.accept(v) },
		"contract": func(r *Registration) { v := record(10, 1, 1); v.Value = []byte(`{}`); r.accept(v) },
		"boundary": func(r *Registration) { r.accept(record(10, 1, 2)); r.accept(marker(11)) },
		"sequence": func(r *Registration) { r.accept(record(10, 1, 3)); r.accept(record(11, 3, 3)) },
		"producer": func(r *Registration) { r.accept(record(10, 1, 2)); v := record(11, 2, 2); v.ProducerID++; r.accept(v) },
		"overflow": func(r *Registration) { r.accept(record(10, 1, 3)) },
		"queue": func(r *Registration) {
			r.accept(record(10, 1, 1))
			r.accept(marker(11))
			r.accept(record(12, 2, 2))
			r.accept(marker(13))
			r.accept(record(14, 3, 3))
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			r := local(2)
			defer r.Close()
			run(r)
			if !errors.Is(r.Err(), ErrContinuity) {
				t.Fatal(r.Err())
			}
			select {
			case <-r.Done():
			default:
				t.Fatal("not revoked")
			}
			if _, _, err := r.TryNext(); err == nil {
				t.Fatal("failed source emitted work")
			}
		})
	}
}

func TestCommitMarkerEpochVersions(t *testing.T) {
	for _, epoch := range []int16{1, 2, 3} {
		r := local(2)
		r.accept(record(10, 1, 1))
		v := marker(11)
		v.ProducerEpoch = epoch
		r.accept(v)
		if epoch < 3 {
			tx, ok, err := r.TryNext()
			if !ok || err != nil || tx.C != 1 {
				t.Fatal(epoch, ok, err)
			}
		} else if !errors.Is(r.Err(), ErrBoundary) {
			t.Fatal(r.Err())
		}
		r.Close()
	}
}
