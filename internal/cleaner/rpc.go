package cleaner

import (
	"bytes"
	"context"
	"os"

	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/matcher/v1"
	npb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/notifications/v1"
	mrpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/progressrpc"
	nrpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"google.golang.org/grpc"
)

// Endpoint contains only RPC client credentials; never owner/SQL credentials.
// It must not be logged. ServerName is always explicitly verified.
type Endpoint struct{ Address, ServerName, CAFile, CertFile, KeyFile string }
type RPCReader struct{ conns map[string]*grpc.ClientConn }

func NewRPCReader(endpoints map[string]Endpoint) (*RPCReader, error) {
	if len(endpoints) < 1 || len(endpoints) > maxConsumers {
		return nil, ErrConfig
	}
	r := &RPCReader{conns: map[string]*grpc.ClientConn{}}
	fail := func() (*RPCReader, error) { r.Close(); return nil, ErrConfig }
	for name, ep := range endpoints {
		if !bounded(name) || ep.Address == "" || ep.ServerName == "" {
			return fail()
		}
		cert, e := os.ReadFile(ep.CertFile)
		if e != nil {
			return fail()
		}
		key, e := os.ReadFile(ep.KeyFile)
		if e != nil {
			return fail()
		}
		ca, e := os.ReadFile(ep.CAFile)
		if e != nil {
			return fail()
		}
		creds, e := serviceauth.ClientCredentials(cert, key, ca)
		if e != nil {
			return fail()
		}
		conn, e := grpc.NewClient(ep.Address, grpc.WithTransportCredentials(creds), grpc.WithAuthority(ep.ServerName))
		if e != nil {
			return fail()
		}
		r.conns[name] = conn
	}
	return r, nil
}
func (r *RPCReader) Close() {
	for _, c := range r.conns {
		_ = c.Close()
	}
}
func (r *RPCReader) Next(ctx context.Context, c Consumer, source kafkasource.Identity, partition int32) (int64, error) {
	if r == nil || !c.valid() || source.Validate() != nil || partition < 0 {
		return 0, ErrProof
	}
	conn := r.conns[c.Endpoint]
	if conn == nil {
		return 0, ErrProof
	}
	if c.Kind == "notifications" {
		out, e := nrpc.NewClient(conn).ReadProgress(ctx, &npb.ReadProgressRequest{ConsumerId: c.ID, Generation: c.Generation, ClusterId: source.ClusterID, TopicId: source.TopicID[:], Topic: source.Topic, Partition: partition})
		if e != nil || out == nil || out.NextOffset == nil || *out.NextOffset < 0 || out.ConsumerId != c.ID || out.Generation != c.Generation || out.ClusterId != source.ClusterID || !bytes.Equal(out.TopicId, source.TopicID[:]) || out.Topic != source.Topic || out.Partition != partition {
			return 0, ErrProof
		}
		return *out.NextOffset, nil
	}
	kind := mpb.ConsumerKind_CONSUMER_KIND_PRICES
	if c.Kind == "projection" {
		kind = mpb.ConsumerKind_CONSUMER_KIND_PROJECTION
	}
	out, e := mrpc.NewClient(conn).ReadProgress(ctx, &mpb.ReadProgressRequest{Kind: kind, ConsumerId: c.ID, ClusterId: source.ClusterID, TopicId: source.TopicID[:], Topic: source.Topic, Partition: partition})
	if e != nil || out == nil || out.NextOffset == nil || *out.NextOffset < 0 || out.Kind != kind || out.ConsumerId != c.ID || out.ClusterId != source.ClusterID || !bytes.Equal(out.TopicId, source.TopicID[:]) || out.Topic != source.Topic || out.Partition != partition {
		return 0, ErrProof
	}
	return *out.NextOffset, nil
}
