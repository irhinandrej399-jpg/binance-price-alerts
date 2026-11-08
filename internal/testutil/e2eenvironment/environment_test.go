// Package e2eenvironment qualifies fresh disposable infrastructure, not product runtimes.
package e2eenvironment

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var owners = map[string]string{
	"api": "DATABASE_URL", "market_data": "MARKET_DATA_DATABASE_URL",
	"matcher": "MATCHER_DATABASE_URL", "notifications": "NOTIFICATIONS_DATABASE_URL",
}

func verifyDB(ctx context.Context, owner, dsn string) error {
	pool, err := postgres.Open(ctx, dsn)
	if err != nil {
		return errors.New("owner database unavailable")
	}
	defer pool.Close()
	var db, role string
	if err = pool.QueryRow(ctx, "SELECT current_database(),current_user").Scan(&db, &role); err != nil || db != owner || role != owner+"_runtime" {
		return errors.New("wrong database identity")
	}
	for foreign := range owners {
		if foreign == owner {
			continue
		}
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			return errors.New("invalid owner configuration")
		}
		cfg.ConnConfig.Database = foreign
		other, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return errors.New("foreign pool configuration failed")
		}
		err = other.Ping(ctx)
		other.Close()
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
			return errors.New("foreign CONNECT was not denied")
		}
	}
	return nil
}

// Only a connectivity child; Env is an explicit allowlist containing one DSN.
func TestChild(t *testing.T) {
	owner := os.Getenv("E2E_ENVIRONMENT_CHILD")
	if owner == "" {
		return
	}
	key, ok := owners[owner]
	if !ok {
		os.Exit(24)
	}
	for _, foreign := range owners {
		if foreign != key && os.Getenv(foreign) != "" {
			os.Exit(25)
		}
	}
	if os.Getenv("PARENT_SECRET") != "" || os.Getenv("E2E_PROCESS_INTEGRATION") != "" {
		os.Exit(26)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if verifyDB(ctx, owner, os.Getenv(key)) != nil {
		os.Exit(27)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	_ = http.ListenAndServe(os.Getenv("HTTP_ADDR"), mux)
	os.Exit(28)
}

func TestDisposableEnvironment(t *testing.T) {
	if os.Getenv("E2E_PROCESS_INTEGRATION") != "1" {
		t.Skip("run python3 scripts/check_e2e_environment.py")
	}
	for _, key := range []string{"DATABASE_URL", "MARKET_DATA_DATABASE_URL", "MATCHER_DATABASE_URL", "NOTIFICATIONS_DATABASE_URL", "KAFKA_TEST_BROKER", "KAFKA_TEST_PREFIX"} {
		if os.Getenv(key) == "" {
			t.Fatal("E2E opt-in requires all four DBs and Kafka")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal("test executable unavailable")
	}
	t.Setenv("PARENT_SECRET", "must-not-leak")
	for owner, key := range owners {
		t.Run(owner, func(t *testing.T) {
			if err := verifyDB(ctx, owner, os.Getenv(key)); err != nil {
				t.Fatal(err)
			}
			err := apiprocess.With(ctx, apiprocess.Config{
				Executable: exe, Args: []string{"-test.run=^TestChild$"},
				Env:            map[string]string{"E2E_ENVIRONMENT_CHILD": owner, key: os.Getenv(key)},
				StartupTimeout: 12 * time.Second, StopTimeout: time.Second,
			}, func(*apiprocess.Process) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	c, err := kgo.NewClient(kgo.SeedBrokers(os.Getenv("KAFKA_TEST_BROKER")))
	if err != nil {
		t.Fatal("Kafka configuration failed")
	}
	defer c.Close()
	meta, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, c)
	if err != nil || meta.ClusterID == nil || *meta.ClusterID == "" {
		t.Fatal("Kafka cluster metadata unavailable")
	}
	expected := map[string]int{os.Getenv("KAFKA_TEST_PREFIX") + "-prices": 16, os.Getenv("KAFKA_TEST_PREFIX") + "-subscription-changes": 1, os.Getenv("KAFKA_TEST_PREFIX") + "-delivery-requests": 16}
	ids := map[[16]byte]bool{}
	for _, topic := range meta.Topics {
		if topic.Topic == nil {
			continue
		}
		count, ok := expected[*topic.Topic]
		if !ok {
			t.Fatal("unexpected topic in fresh dataset")
		}
		if topic.ErrorCode != 0 || topic.TopicID == [16]byte{} || ids[topic.TopicID] || len(topic.Partitions) != count {
			t.Fatal("invalid topic identity/partitions")
		}
		ids[topic.TopicID] = true
		for _, timestamp := range []int64{-2, -1} {
			req := kmsg.NewPtrListOffsetsRequest()
			rt := kmsg.NewListOffsetsRequestTopic()
			rt.Topic = *topic.Topic
			for _, p := range topic.Partitions {
				if p.ErrorCode != 0 {
					t.Fatal("partition unavailable")
				}
				rp := kmsg.NewListOffsetsRequestTopicPartition()
				rp.Partition = p.Partition
				rp.Timestamp = timestamp
				rt.Partitions = append(rt.Partitions, rp)
			}
			req.Topics = append(req.Topics, rt)
			response, err := req.RequestWith(ctx, c)
			if err != nil || len(response.Topics) != 1 || len(response.Topics[0].Partitions) != count {
				t.Fatal("offset discovery failed")
			}
			for _, p := range response.Topics[0].Partitions {
				if p.ErrorCode != 0 || p.Offset != 0 {
					t.Fatal("dataset is not new and empty")
				}
			}
		}
		t.Logf("fresh topic %s id=%x partitions=%d", *topic.Topic, topic.TopicID, count)
		delete(expected, *topic.Topic)
	}
	if len(expected) != 0 {
		t.Fatal("missing application topic")
	}
	t.Log("four owner databases/child allowlists and fresh Kafka qualified")
}
