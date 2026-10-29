package projection

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"google.golang.org/protobuf/proto"
)

func legacyImage(t *testing.T) (changed.Changed, []byte) {
	t.Helper()
	var c changed.Changed
	raw := []byte(`{"schema_version":1,"position":1,"transaction_end":1,"subscription_id":"sub","subscription_version":1,"state":"active","pair":"BTCUSDT","threshold":"100","direction":"up","activation":"latest_price","creation_snapshot":{"price":"100","trade_time":"2026-10-29T09:00:00Z"}}`)
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c, raw
}

func TestLegacyImageRequiresBootstrap(t *testing.T) {
	_, raw := legacyImage(t)
	if _, err := decodeImage(raw, nil); !errors.Is(err, ErrOwnerMissing) {
		t.Fatal(err)
	}
	snap, err := proto.Marshal(&pb.SnapshotSubscription{SubscriptionId: "sub"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeImage(nil, snap); !errors.Is(err, ErrOwnerMissing) {
		t.Fatal(err)
	}
}

func TestLegacyTransactionNeverAcknowledged(t *testing.T) {
	_, payload := legacyImage(t)
	raw := append([]byte(`{"event_id":"old","type":"subscription.changed","version":1,"occurred_at":"2026-10-29T10:00:00Z","correlation_id":"old","ordering_key":"sub","payload":`), payload...)
	raw = append(raw, '}')
	s := New(nil, "legacy") // No SQL access should occur for this invalid candidate.
	if err := s.Handle(context.Background(), Batch{Topic: "changes", NextOffset: 1, Events: [][]byte{raw}}, func(context.Context, int64) error { t.Fatal("legacy acknowledged"); return nil }); !errors.Is(err, ErrOwnerMissing) {
		t.Fatal(err)
	}
	if err := s.ApplyReplay(context.Background(), [][]byte{raw}); !errors.Is(err, ErrOwnerMissing) {
		t.Fatal(err)
	}
}

func TestOwnerFromBothImageForms(t *testing.T) {
	c, _ := legacyImage(t)
	c.SchemaVersion, c.OwnerID = changed.Version2, "canonical"
	for _, im := range []Image{{Change: &c}, {Snapshot: &pb.SnapshotSubscription{OwnerId: "canonical"}}} {
		owner, err := im.OwnerID()
		if err != nil || owner != "canonical" {
			t.Fatal(owner, err)
		}
	}
	for _, im := range []Image{{}, {Change: &c, Snapshot: &pb.SnapshotSubscription{OwnerId: "other"}}} {
		if owner, err := im.OwnerID(); owner != "" || !errors.Is(err, ErrOwnerMissing) {
			t.Fatal(owner, err)
		}
	}
}
