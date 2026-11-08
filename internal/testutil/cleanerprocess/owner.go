package cleanerprocess

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"time"

	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/matcher/v1"
	npb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/notifications/v1"
	mrpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/progressrpc"
	nrpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"google.golang.org/grpc"
)

var errOwner = errors.New("fixture owner configuration or startup failed")

// ServeOwner runs only the real protected read-only SQL exporter in a test
// executable. It is not cmd/matcher or cmd/notifications and runs no workers.
// apiprocess owns process groups, signals, startup probes and final reaping.
func ServeOwner(ctx context.Context) error {
	role := os.Getenv("CLEANER_FIXTURE_ROLE")
	if role != "matcher" && role != "notifications" {
		return errOwner
	}
	for _, key := range []string{"DATABASE_URL", "MATCHER_DATABASE_URL", "NOTIFICATIONS_DATABASE_URL", "MARKET_DATA_DATABASE_URL", "PARENT_SECRET"} {
		if os.Getenv(key) != "" {
			return errOwner
		}
	}
	start, cancel := context.WithTimeout(ctx, 5*time.Second)
	pool, e := postgres.Open(start, os.Getenv("OWNER_DATABASE_URL"))
	cancel()
	if e != nil {
		return errOwner
	}
	defer pool.Close()
	start, cancel = context.WithTimeout(ctx, 3*time.Second)
	var name string
	e = pool.QueryRow(start, `SELECT current_user`).Scan(&name)
	if e != nil || name != role+"_runtime" {
		cancel()
		return errOwner
	}
	for _, db := range []string{"api", "matcher", "notifications", "market_data"} {
		if db == role {
			continue
		}
		var allowed bool
		e = pool.QueryRow(start, `SELECT has_database_privilege(current_user,$1,'CONNECT')`, db).Scan(&allowed)
		if e != nil || allowed {
			cancel()
			return errOwner
		}
	}
	cancel()
	read := func(key string) []byte { b, _ := os.ReadFile(os.Getenv(key)); return b }
	cert, key, ca := read("RPC_CERT_FILE"), read("RPC_KEY_FILE"), read("RPC_CA_FILE")
	var opts []grpc.ServerOption
	if role == "matcher" {
		opts, e = mrpc.ServerOptions(cert, key, ca, CleanerURI)
	} else {
		opts, e = nrpc.ServerOptions(cert, key, ca, CleanerURI)
	}
	if e != nil {
		return errOwner
	}
	rpc, e := net.Listen("tcp", os.Getenv("RPC_ADDR"))
	if e != nil {
		return errOwner
	}
	defer rpc.Close()
	httpLis, e := net.Listen("tcp", os.Getenv("HTTP_ADDR"))
	if e != nil {
		return errOwner
	}
	defer httpLis.Close()
	g := grpc.NewServer(opts...)
	if role == "matcher" {
		mpb.RegisterMatcherProgressServer(g, mrpc.NewServer(mrpc.NewPostgres(pool)))
	} else {
		npb.RegisterNotificationsProgressServer(g, nrpc.NewServer(nrpc.NewPostgres(pool)))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		c, stop := context.WithTimeout(r.Context(), time.Second)
		defer stop()
		if pool.Ping(c) != nil {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	h := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	done := make(chan error, 2)
	go func() { done <- g.Serve(rpc) }()
	go func() { done <- h.Serve(httpLis) }()
	var result error
	select {
	case <-ctx.Done():
	case <-done:
		result = errOwner
	}
	// Stop joins the bounded exporter handlers before closing its owner pool.
	g.Stop()
	_ = h.Close()
	if result == nil {
		<-done
	}
	<-done
	return result
}
