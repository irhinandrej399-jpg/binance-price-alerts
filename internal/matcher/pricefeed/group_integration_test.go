package pricefeed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/publish"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func groupFixture(t *testing.T, count int32, buffer int) (GroupConfig, *kgo.Client) {
	t.Helper()
	cfg, p := integration(t, 0, buffer)
	cfg.Partitions = nil
	cfg.Topic = strings.TrimSuffix(cfg.Topic, "-prices") + "-" + strings.ReplaceAll(strings.ToLower(t.Name()), "/", "-")
	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = cfg.Topic
	topic.NumPartitions = count
	topic.ReplicationFactor = 1
	req.Topics = []kmsg.CreateTopicsRequestTopic{topic}
	res, err := req.RequestWith(bounded(t, 5*time.Second), p)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Topics) != 1 || res.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic: %+v", res.Topics)
	}
	return GroupConfig{Config: cfg, Group: cfg.Topic + "-owners", Prepare: func(ctx context.Context, parts []int32) (map[int32]int64, error) {
		out := map[int32]int64{}
		for _, part := range parts {
			out[part] = 0
		}
		return out, ctx.Err()
	}}, p
}
func group(t *testing.T, cfg GroupConfig) *Group {
	t.Helper()
	g, err := NewGroup(bounded(t, 60*time.Second), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	return g
}
func assignment(t *testing.T, g *Group, after *Assignment) *Assignment {
	t.Helper()
	a, err := g.NextAssignment(bounded(t, 15*time.Second), after)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func pairAt(part int32) string {
	for i := 0; ; i++ {
		s := fmt.Sprintf("PAIR%dUSDT", i)
		if PairPartition(s) == part {
			return s
		}
	}
}
func payloadFor(pair string) []byte {
	return bytes.ReplaceAll(fixture(), []byte("BTCUSDT"), []byte(pair))
}
func groupProduce(t *testing.T, p *kgo.Client, cfg GroupConfig, pair string, commit bool) *kgo.Record {
	t.Helper()
	c := cfg.Config
	c.Partitions = []int32{PairPartition(pair)}
	return produce(t, p, c, payloadFor(pair), pair, commit)
}
func allNext(t *testing.T, a *Assignment) Update {
	t.Helper()
	u, err := a.Next(bounded(t, 8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return u
}
func confirm(t *testing.T, a *Assignment, u Update) {
	t.Helper()
	if err := a.Confirm(bounded(t, 5*time.Second), u.Position); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationGroupTwoMembersAndRevoke(t *testing.T) {
	cfg, p := groupFixture(t, 16, 256)
	g1 := group(t, cfg)
	old := assignment(t, g1, nil)
	if len(old.Starts()) != 16 {
		t.Fatal(old.Starts())
	}
	pair := pairAt(0)
	r, err := old.Register(bounded(t, 30*time.Second), pair, 0)
	if err != nil {
		t.Fatal(err)
	}
	first := groupProduce(t, p, cfg, pair, true)
	u := allNext(t, old)
	if u.Position.Offset != first.Offset {
		t.Fatal(u)
	}
	// No implicit commit despite polling/delivery.
	fetch := kmsg.NewPtrOffsetFetchRequest()
	fetch.Group = cfg.Group
	ft := kmsg.NewOffsetFetchRequestTopic()
	ft.Topic = cfg.Topic
	ft.Partitions = []int32{0}
	fetch.Topics = []kmsg.OffsetFetchRequestTopic{ft}
	res, err := fetch.RequestWith(bounded(t, 5*time.Second), p)
	if err != nil {
		t.Fatal(err)
	}
	if res.Topics[0].Partitions[0].Offset != -1 {
		t.Fatalf("auto commit: %+v", res)
	}
	g2 := group(t, cfg)
	select {
	case <-old.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("no revoke")
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, ErrRevoked) {
		t.Fatal(err)
	}
	if err := old.Confirm(context.Background(), u.Position); !errors.Is(err, ErrRevoked) {
		t.Fatal(err)
	}
	if _, err := old.Next(context.Background()); !errors.Is(err, ErrRevoked) {
		t.Fatal(err)
	}
	a1 := assignment(t, g1, old)
	a2 := assignment(t, g2, nil)
	seen := map[int32]bool{}
	for _, a := range []*Assignment{a1, a2} {
		for part := int32(0); part < 16; part++ {
			if _, own := a.Starts()[part]; !own {
				if _, err := a.Register(context.Background(), pairAt(part), part); !errors.Is(err, ErrConfig) {
					t.Fatal("foreign registration", err)
				}
				if err := a.Confirm(context.Background(), Position{cfg.Topic, part, 0}); !errors.Is(err, ErrConfirm) {
					t.Fatal("foreign confirmation", err)
				}
				break
			}
		}
		for part := range a.Starts() {
			if seen[part] {
				t.Fatal("overlap", part)
			}
			seen[part] = true
		}
	}
	if len(seen) != 16 || len(a1.Starts()) == 0 || len(a2.Starts()) == 0 {
		t.Fatal("incomplete", seen)
	}
	// All 16 partitions, including pairs with no registrations; stable mapping
	// checked against the actual production adapter's partitioner.
	prod, err := publish.New(bounded(t, 8*time.Second), kafka.Config{Brokers: cfg.Brokers, TransactionalID: cfg.Topic + "-production"}, cfg.Topic)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	for part := int32(0); part < 16; part++ {
		if err := prod.Publish(bounded(t, 8*time.Second), payloadFor(pairAt(part))); err != nil {
			t.Fatal(err)
		}
	}
	for _, a := range []*Assignment{a1, a2} {
		wanted := len(a.Starts())
		got := map[int32]bool{}
		for len(got) < wanted {
			v := allNext(t, a)
			if _, ok := a.Starts()[v.Position.Partition]; !ok || PairPartition(v.Key) != v.Position.Partition {
				t.Fatal("foreign partition", v)
			}
			confirm(t, a, v)
			// The record before rebalance is replayed from caller's 0 as well.
			if !(v.Position.Partition == 0 && v.Position.Offset == first.Offset) {
				got[v.Position.Partition] = true
			}
		}
		if _, err := a.Register(context.Background(), pairAt(15), 0); !errors.Is(err, ErrConfig) {
			t.Fatal(err)
		}
	}
	g2.Close()
	select {
	case <-a1.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("close did not rebalance")
	}
	next := assignment(t, g1, a1)
	if len(next.Starts()) != 16 {
		t.Fatal(next.Starts())
	}
	g1.Close()
	if _, err := next.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestIntegrationGroupBarrierIsolationAndConfirm(t *testing.T) {
	cfg, p := groupFixture(t, 16, 32)
	g := group(t, cfg)
	a := assignment(t, g, nil)
	pair := "BTCUSDT"
	part := PairPartition(pair)
	ctx := bounded(t, 8*time.Second)
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	open := &kgo.Record{Topic: cfg.Topic, Partition: part, Key: []byte(pair), Value: fixture()}
	if err := p.ProduceSync(ctx, open).FirstErr(); err != nil {
		t.Fatal(err)
	}
	r, err := a.Register(bounded(t, 20*time.Second), pair, part)
	if err != nil {
		t.Fatal(err)
	}
	if r.Start().Offset > open.Offset {
		t.Fatal("LSO skipped open transaction")
	}
	if _, err := a.Next(bounded(t, 250*time.Millisecond)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("open visible", err)
	}
	if err := p.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	u := allNext(t, a)
	v := next(t, r)
	if u.Position != v.Position || u.Position.Offset != open.Offset || u.Envelope.EventID != "original-event" || u.Quote.Price.String() != "123.456789012345678901" {
		t.Fatal(u, v)
	}
	bad := u.Position
	bad.Offset++
	if err := a.Confirm(ctx, bad); !errors.Is(err, ErrConfirm) {
		t.Fatal(err)
	}
	groupProduce(t, p, cfg, pair, false)
	sent := groupProduce(t, p, cfg, pair, true)
	u2 := allNext(t, a)
	if u2.Position.Offset != sent.Offset {
		t.Fatal("abort visible", u2)
	}
	if err := a.Confirm(ctx, u2.Position); !errors.Is(err, ErrConfirm) {
		t.Fatal("out of order ack", err)
	}
	confirm(t, a, u)
	confirm(t, a, u2)
	if err := a.Confirm(ctx, u.Position); !errors.Is(err, ErrConfirm) {
		t.Fatal("duplicate ack", err)
	}
	fetch := kmsg.NewPtrOffsetFetchRequest()
	fetch.Group = cfg.Group
	ft := kmsg.NewOffsetFetchRequestTopic()
	ft.Topic = cfg.Topic
	ft.Partitions = []int32{part}
	fetch.Topics = []kmsg.OffsetFetchRequestTopic{ft}
	res, err := fetch.RequestWith(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Topics[0].Partitions[0].Offset; got != sent.Offset+1 {
		t.Fatal(got, sent.Offset)
	}
	// Register is compatible with MAT-012's registration interface; revoke
	// invalidates even already queued pair records and confirmations.
	g.Close()
	select {
	case <-r.Done():
	default:
		t.Fatal("registration survives close")
	}
	if _, _, err := r.TryNext(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	// A replacement ignores the broker offset and starts at the caller's exact
	// checkpoint. This replays the original event even after a broker commit.
	replacement := group(t, cfg)
	replay := assignment(t, replacement, nil)
	if got := allNext(t, replay); got.Position != u.Position {
		t.Fatal("broker offset replaced caller checkpoint", got)
	}
}

func TestIntegrationGroupLost(t *testing.T) {
	cfg, p := groupFixture(t, 16, 32)
	g := group(t, cfg)
	a := assignment(t, g, nil)
	pair := "BTCUSDT"
	r, err := a.Register(bounded(t, 20*time.Second), pair, PairPartition(pair))
	if err != nil {
		t.Fatal(err)
	}
	groupProduce(t, p, cfg, pair, true)
	u := allNext(t, a)
	member, _ := g.client.GroupMetadata()
	req := kmsg.NewPtrLeaveGroupRequest()
	req.Group = cfg.Group
	req.MemberID = member
	m := kmsg.NewLeaveGroupRequestMember()
	m.MemberID = member
	req.Members = []kmsg.LeaveGroupRequestMember{m}
	resp, err := req.RequestWith(bounded(t, 5*time.Second), p)
	if err != nil || resp.ErrorCode != 0 {
		t.Fatal(resp, err)
	}
	select {
	case <-a.Done():
	case <-time.After(8 * time.Second):
		t.Fatal("lost member kept authority")
	}
	if !errors.Is(a.Err(), ErrLost) {
		t.Fatal(a.Err())
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, ErrLost) {
		t.Fatal(err)
	}
	if err := a.Confirm(context.Background(), u.Position); !errors.Is(err, ErrLost) {
		t.Fatal(err)
	}
}

func TestIntegrationGroupMetadataChange(t *testing.T) {
	cfg, p := groupFixture(t, 16, 32)
	g := group(t, cfg)
	a := assignment(t, g, nil)
	req := kmsg.NewPtrCreatePartitionsRequest()
	v := kmsg.NewCreatePartitionsRequestTopic()
	v.Topic = cfg.Topic
	v.Count = 17
	req.Topics = []kmsg.CreatePartitionsRequestTopic{v}
	resp, err := req.RequestWith(bounded(t, 5*time.Second), p)
	if err != nil || len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatal(resp, err)
	}
	select {
	case <-g.Done():
	case <-time.After(8 * time.Second):
		t.Fatal("accepted changed partition count")
	}
	if !errors.Is(g.Err(), ErrConfig) {
		t.Fatal(g.Err())
	}
	if _, err := a.Next(context.Background()); err == nil {
		t.Fatal("old mapping survived")
	}
}

func TestIntegrationGroupDeletedCursor(t *testing.T) {
	cfg, p := groupFixture(t, 16, 32)
	pair := "BTCUSDT"
	part := PairPartition(pair)
	groupProduce(t, p, cfg, pair, true)
	req := kmsg.NewPtrDeleteRecordsRequest()
	v := kmsg.NewDeleteRecordsRequestTopic()
	v.Topic = cfg.Topic
	w := kmsg.NewDeleteRecordsRequestTopicPartition()
	w.Partition = part
	w.Offset = -1
	v.Partitions = []kmsg.DeleteRecordsRequestTopicPartition{w}
	req.Topics = []kmsg.DeleteRecordsRequestTopic{v}
	resp, err := req.RequestWith(bounded(t, 5*time.Second), p)
	if err != nil || len(resp.Topics) != 1 || resp.Topics[0].Partitions[0].ErrorCode != 0 || resp.Topics[0].Partitions[0].LowWatermark <= 0 {
		t.Fatal(resp, err)
	}
	g := group(t, cfg)
	if a, err := g.NextAssignment(bounded(t, 8*time.Second), nil); a != nil || !errors.Is(err, ErrPosition) {
		t.Fatal("deleted cursor reset", a, err)
	}
}

func TestIntegrationGroupFailureBounds(t *testing.T) {
	for _, mode := range []string{"overflow", "unconfirmed", "contract", "cursor", "prepare", "deadline", "close-prepare", "count", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			count := int32(16)
			if mode == "count" {
				count = 1
			}
			cfg, p := groupFixture(t, count, 1)
			entered := make(chan struct{})
			if mode == "prepare" {
				cfg.Prepare = func(context.Context, []int32) (map[int32]int64, error) { return nil, errors.New("secret") }
			}
			if mode == "deadline" {
				cfg.Prepare = func(ctx context.Context, _ []int32) (map[int32]int64, error) { <-ctx.Done(); return nil, ctx.Err() }
			}
			if mode == "close-prepare" {
				cfg.Prepare = func(ctx context.Context, _ []int32) (map[int32]int64, error) {
					close(entered)
					<-ctx.Done()
					return nil, ctx.Err()
				}
			}
			if mode == "cursor" {
				cfg.Prepare = func(_ context.Context, parts []int32) (map[int32]int64, error) {
					m := map[int32]int64{}
					for _, p := range parts {
						m[p] = 1 << 60
					}
					return m, nil
				}
			}
			if mode == "count" {
				if g, err := NewGroup(bounded(t, 8*time.Second), cfg); !errors.Is(err, ErrConfig) {
					if g != nil {
						g.Close()
					}
					t.Fatal(err)
				}
				return
			}
			owner, cancel := context.WithCancel(bounded(t, 30*time.Second))
			defer cancel()
			g, err := NewGroup(owner, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			if mode == "close-prepare" {
				select {
				case <-entered:
				case <-time.After(8 * time.Second):
					t.Fatal("no preparation")
				}
				closed := make(chan struct{})
				go func() { g.Close(); close(closed) }()
				select {
				case <-closed:
				case <-time.After(2 * time.Second):
					t.Fatal("Close blocked by preparation")
				}
				return
			}
			if mode == "prepare" || mode == "deadline" || mode == "cursor" {
				select {
				case <-g.Done():
				case <-time.After(12 * time.Second):
					t.Fatal("failure did not stop group")
				}
				if g.Err() == nil {
					t.Fatal("missing failure")
				}
				return
			}
			a := assignment(t, g, nil)
			if mode == "cancel" {
				cancel()
				select {
				case <-g.Done():
				case <-time.After(time.Second):
					t.Fatal("cancel stuck")
				}
				return
			}
			pair := "BTCUSDT"
			if mode == "contract" {
				c := cfg.Config
				c.Partitions = []int32{PairPartition(pair)}
				produce(t, p, c, []byte("{"), pair, true)
			} else {
				groupProduce(t, p, cfg, pair, true)
				if mode == "unconfirmed" {
					allNext(t, a)
				}
				groupProduce(t, p, cfg, pair, true)
			}
			select {
			case <-g.Done():
			case <-time.After(8 * time.Second):
				t.Fatal("bound not enforced")
			}
			want := ErrOverflow
			if mode == "contract" {
				want = ErrContract
			}
			if _, err := a.Next(context.Background()); !errors.Is(err, want) {
				t.Fatal(err)
			}
		})
	}
}
