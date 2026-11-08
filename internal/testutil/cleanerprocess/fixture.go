// Package cleanerprocess assembles disposable SQL exporters and protected RPC
// fault boundaries. It contains no cleaner algorithm or consumer registry.
package cleanerprocess

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/matcher/v1"
	npb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/notifications/v1"
	mrpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/progressrpc"
	nrpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/kafkafault"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"google.golang.org/grpc"
)

const CleanerURI = "spiffe://cleaner.test/cleaner"
const serverName = "market.test"

// Caller is the entire caller capability. No DSNs, owner certificate or CA key.
// File paths point only to short-lived credentials. Never print this structure.
type Caller struct {
	Broker                 string
	Matcher, Notifications Endpoint
}
type Endpoint struct{ Address, CAFile, CertFile, KeyFile string }

func (e Endpoint) Conn(t testing.TB) *grpc.ClientConn {
	t.Helper()
	cert, err := os.ReadFile(e.CertFile)
	if err != nil {
		t.Fatal("read caller certificate")
	}
	key, err := os.ReadFile(e.KeyFile)
	if err != nil {
		t.Fatal("read caller key")
	}
	ca, err := os.ReadFile(e.CAFile)
	if err != nil {
		t.Fatal("read caller CA")
	}
	creds, err := serviceauth.ClientCredentials(cert, key, ca)
	if err != nil {
		t.Fatal("caller credentials")
	}
	c, err := grpc.NewClient(e.Address, grpc.WithTransportCredentials(creds), grpc.WithAuthority(serverName))
	if err != nil {
		t.Fatal("caller connection")
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func (c Caller) Env() map[string]string {
	return map[string]string{"KAFKA_BROKERS": c.Broker,
		"MATCHER_RPC_ADDR": c.Matcher.Address, "MATCHER_CA_FILE": c.Matcher.CAFile, "MATCHER_CERT_FILE": c.Matcher.CertFile, "MATCHER_KEY_FILE": c.Matcher.KeyFile,
		"NOTIFICATIONS_RPC_ADDR": c.Notifications.Address, "NOTIFICATIONS_CA_FILE": c.Notifications.CAFile, "NOTIFICATIONS_CERT_FILE": c.Notifications.CertFile, "NOTIFICATIONS_KEY_FILE": c.Notifications.KeyFile}
}

// Kafka opens a separate fault proxy per caller. Fault behavior is wholly owned
// by OPS-068; the fixture does not decode Kafka frames or synthesize responses.
func (c Caller) Kafka(t testing.TB) (*kgo.Client, *kafkafault.Proxy) {
	t.Helper()
	p := kafkafault.New(c.Broker)
	t.Cleanup(p.Close)
	k, e := kgo.NewClient(kgo.SeedBrokers(c.Broker), kgo.MaxVersions(kversion.V2_8_0()), kgo.Dialer(p.DialContext), kgo.RequestTimeoutOverhead(time.Second))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(k.Close)
	return k, p
}

type owner struct {
	address, cert, key, ca string
	gate                   *gates
}
type Fixture struct {
	Caller Caller
	owners map[string]owner
	denied map[string]Endpoint
}

// New builds two independent TLS trust domains and forwarding gates. SQL
// credentials are supplied only when configuring the corresponding owner child.
func New(t testing.TB, broker string) *Fixture {
	t.Helper()
	f := &Fixture{Caller: Caller{Broker: broker}, owners: map[string]owner{}, denied: map[string]Endpoint{}}
	for _, role := range []string{"matcher", "notifications"} {
		ca := newAuthority(t)
		server := ca.issue(t, nil, true, false)
		cleaner := ca.issue(t, []string{CleanerURI}, false, false)
		outsider := ca.issue(t, []string{"spiffe://cleaner.test/outsider"}, false, false)
		dir := t.TempDir()
		write := func(name string, b []byte) string {
			path := filepath.Join(dir, name)
			if os.WriteFile(path, b, 0600) != nil {
				t.Fatal("write ephemeral credential")
			}
			return path
		}
		o := owner{address: freeAddress(t), cert: write("owner.pem", server.cert), key: write("owner.key", server.key), ca: write("ca.pem", ca.pem), gate: &gates{}}
		endpoint := Endpoint{Address: o.address, CAFile: o.ca, CertFile: write("cleaner.pem", cleaner.cert), KeyFile: write("cleaner.key", cleaner.key)}
		downstream := endpoint.Conn(t)
		var opts []grpc.ServerOption
		var err error
		if role == "matcher" {
			opts, err = mrpc.ServerOptions(server.cert, server.key, ca.pem, CleanerURI)
		} else {
			opts, err = nrpc.ServerOptions(server.cert, server.key, ca.pem, CleanerURI)
		}
		if err != nil {
			t.Fatal("exporter credentials")
		}
		opts = append(opts, grpc.ChainUnaryInterceptor(o.gate.intercept))
		proxy := grpctest.StartWithClientOptions(t, func(g *grpc.Server) {
			if role == "matcher" {
				mpb.RegisterMatcherProgressServer(g, &matcherForward{client: mrpc.NewClient(downstream)})
			} else {
				npb.RegisterNotificationsProgressServer(g, &notificationsForward{client: nrpc.NewClient(downstream)})
			}
		}, dialOptions(t, cleaner, ca.pem), opts...)
		endpoint.Address = proxy.Target()
		f.denied[role] = Endpoint{Address: endpoint.Address, CAFile: o.ca, CertFile: write("outsider.pem", outsider.cert), KeyFile: write("outsider.key", outsider.key)}
		if role == "matcher" {
			f.Caller.Matcher = endpoint
		} else {
			f.Caller.Notifications = endpoint
		}
		f.owners[role] = o
		t.Cleanup(o.gate.release)
	}
	return f
}
func dialOptions(t testing.TB, p pair, ca []byte) []grpc.DialOption {
	t.Helper()
	creds, e := serviceauth.ClientCredentials(p.cert, p.key, ca)
	if e != nil {
		t.Fatal("client credentials")
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithAuthority(serverName)}
}
func freeAddress(t testing.TB) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	a := l.Addr().String()
	_ = l.Close()
	return a
}

// OwnerConfig uses the existing process lifecycle. executable must call
// ServeOwner in the selected test helper mode. dsn belongs only to this role.
func (f *Fixture) OwnerConfig(role, dsn, executable string, args ...string) apiprocess.Config {
	o, ok := f.owners[role]
	if !ok {
		return apiprocess.Config{}
	}
	return apiprocess.Config{Executable: executable, Args: args, StartupTimeout: 8 * time.Second, StopTimeout: 3 * time.Second, Env: map[string]string{
		"CLEANER_FIXTURE_ROLE": role, "OWNER_DATABASE_URL": dsn, "RPC_ADDR": o.address, "RPC_CERT_FILE": o.cert, "RPC_KEY_FILE": o.key, "RPC_CA_FILE": o.ca}}
}

// PauseRPC selects one call on this role's test forwarding endpoint. after=true
// holds a genuine successful SQL exporter response before delivering it to caller.
func (f *Fixture) PauseRPC(role string, after bool) *Barrier {
	o, ok := f.owners[role]
	if !ok {
		panic("unknown fixture owner")
	}
	return o.gate.pause(after)
}

type matcherForward struct {
	mpb.UnimplementedMatcherProgressServer
	client *mrpc.Client
}

func (s *matcherForward) ReadProgress(ctx context.Context, q *mpb.ReadProgressRequest) (*mpb.ReadProgressResponse, error) {
	return s.client.ReadProgress(ctx, q)
}

type notificationsForward struct {
	npb.UnimplementedNotificationsProgressServer
	client *nrpc.Client
}

func (s *notificationsForward) ReadProgress(ctx context.Context, q *npb.ReadProgressRequest) (*npb.ReadProgressResponse, error) {
	return s.client.ReadProgress(ctx, q)
}
