package cleaner

import (
	"context"
	"errors"
	"testing"

	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/matcher/v1"
	npb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/notifications/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
)

type matcherReply struct {
	mpb.UnimplementedMatcherProgressServer
	mutate func(*mpb.ReadProgressResponse)
}

func (s matcherReply) ReadProgress(_ context.Context, q *mpb.ReadProgressRequest) (*mpb.ReadProgressResponse, error) {
	zero := int64(0)
	r := &mpb.ReadProgressResponse{Kind: q.Kind, ConsumerId: q.ConsumerId, ClusterId: q.ClusterId, TopicId: q.TopicId, Topic: q.Topic, Partition: q.Partition, NextOffset: &zero}
	if s.mutate != nil {
		s.mutate(r)
	}
	return r, nil
}

type notificationsReply struct {
	npb.UnimplementedNotificationsProgressServer
	mutate func(*npb.ReadProgressResponse)
}

func (s notificationsReply) ReadProgress(_ context.Context, q *npb.ReadProgressRequest) (*npb.ReadProgressResponse, error) {
	zero := int64(0)
	r := &npb.ReadProgressResponse{ConsumerId: q.ConsumerId, Generation: q.Generation, ClusterId: q.ClusterId, TopicId: q.TopicId, Topic: q.Topic, Partition: q.Partition, NextOffset: &zero}
	if s.mutate != nil {
		s.mutate(r)
	}
	return r, nil
}
func TestRejectMismatchedMatcherProof(t *testing.T) {
	cases := map[string]func(*mpb.ReadProgressResponse){"zero valid": nil, "missing": func(r *mpb.ReadProgressResponse) { r.NextOffset = nil }, "negative": func(r *mpb.ReadProgressResponse) { *r.NextOffset = -1 }, "kind": func(r *mpb.ReadProgressResponse) { r.Kind = mpb.ConsumerKind_CONSUMER_KIND_PROJECTION }, "consumer": func(r *mpb.ReadProgressResponse) { r.ConsumerId = "other" }, "cluster": func(r *mpb.ReadProgressResponse) { r.ClusterId = "other" }, "topic": func(r *mpb.ReadProgressResponse) { r.Topic = "other" }, "partition": func(r *mpb.ReadProgressResponse) { r.Partition = 1 }, "topic ID": func(r *mpb.ReadProgressResponse) { r.TopicId = make([]byte, 16) }}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			conn := grpctest.Start(t, func(g *grpc.Server) { mpb.RegisterMatcherProgressServer(g, matcherReply{mutate: mutate}) })
			r := &RPCReader{conns: map[string]*grpc.ClientConn{"matcher": conn}}
			n, e := r.Next(context.Background(), plan().Consumers[0], plan().Source, 0)
			if mutate == nil {
				if e != nil || n != 0 {
					t.Fatal(n, e)
				}
			} else if !errors.Is(e, ErrProof) {
				t.Fatal("forged proof accepted", n, e)
			}
		})
	}
}
func TestRejectMismatchedNotificationsProof(t *testing.T) {
	cases := map[string]func(*npb.ReadProgressResponse){"zero valid": nil, "missing": func(r *npb.ReadProgressResponse) { r.NextOffset = nil }, "negative": func(r *npb.ReadProgressResponse) { *r.NextOffset = -1 }, "generation": func(r *npb.ReadProgressResponse) { r.Generation = "other" }, "consumer": func(r *npb.ReadProgressResponse) { r.ConsumerId = "other" }, "cluster": func(r *npb.ReadProgressResponse) { r.ClusterId = "other" }, "topic": func(r *npb.ReadProgressResponse) { r.Topic = "other" }, "partition": func(r *npb.ReadProgressResponse) { r.Partition = 1 }, "topic ID": func(r *npb.ReadProgressResponse) { r.TopicId = make([]byte, 16) }}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			conn := grpctest.Start(t, func(g *grpc.Server) { npb.RegisterNotificationsProgressServer(g, notificationsReply{mutate: mutate}) })
			r := &RPCReader{conns: map[string]*grpc.ClientConn{"notifications": conn}}
			c := Consumer{Name: "n", Kind: "notifications", ID: "n", Generation: "one", Endpoint: "notifications"}
			n, e := r.Next(context.Background(), c, plan().Source, 0)
			if mutate == nil {
				if e != nil || n != 0 {
					t.Fatal(n, e)
				}
			} else if !errors.Is(e, ErrProof) {
				t.Fatal("forged proof accepted", n, e)
			}
		})
	}
}
