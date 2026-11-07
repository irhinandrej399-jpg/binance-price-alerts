package notificationsprocess

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Observation is a snapshot from an independent notifications reader pool.
// Evidence counts include history; pending reports include all four outcomes.
type Observation struct {
	NextAttemptAt                                                                  pgtype.Timestamptz
	State                                                                          string
	Body                                                                           []byte
	Fence                                                                          int64
	LeaseID                                                                        *string
	LeaseExpiresAt, NextCheckAt                                                    *time.Time
	Claims, Attempts, Successes, Uncertainties, Blocks, Retryables, PendingReports int
}

func Observe(ctx context.Context, reader *pgxpool.Pool, trigger string) (Observation, error) {
	var o Observation
	// One statement gives a consistent observer snapshot without taking job locks.
	e := reader.QueryRow(ctx, `SELECT state,body,lease_fence,lease_id,lease_expires_at,next_check_at,next_attempt_at,
 (SELECT count(*) FROM app.delivery_claims WHERE trigger_id=$1),
 (SELECT count(*) FROM app.delivery_attempts WHERE trigger_id=$1),
 (SELECT count(*) FROM app.delivery_successes s JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1),
 (SELECT count(*) FROM app.delivery_uncertainties s JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1),
 (SELECT count(*) FROM app.delivery_blocks s JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1),
 (SELECT count(*) FROM app.delivery_retryables s JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1),
 (SELECT count(*) FROM (
 SELECT attempt_id FROM app.delivery_success_reports WHERE pending UNION ALL
 SELECT attempt_id FROM app.delivery_uncertain_reports WHERE pending UNION ALL
 SELECT attempt_id FROM app.delivery_blocked_reports WHERE pending UNION ALL
 SELECT attempt_id FROM app.delivery_retryable_reports WHERE pending
 ) p JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1)
 FROM app.delivery_jobs WHERE trigger_id=$1`, trigger).Scan(&o.State, &o.Body, &o.Fence, &o.LeaseID, &o.LeaseExpiresAt, &o.NextCheckAt, &o.NextAttemptAt, &o.Claims, &o.Attempts, &o.Successes, &o.Uncertainties, &o.Blocks, &o.Retryables, &o.PendingReports)
	return o, e
}
func Checkpoint(ctx context.Context, reader *pgxpool.Pool, topic string, partition int32) (int64, error) {
	var n int64
	e := reader.QueryRow(ctx, `SELECT next_offset FROM app.delivery_checkpoints WHERE topic=$1 AND partition=$2`, topic, partition).Scan(&n)
	return n, e
}

// BrokerOffset opens an independent client. It neither commits nor resets offsets.
func (f *Fixture) BrokerOffset(ctx context.Context, group string, partition int32) (int64, error) {
	c, e := kgo.NewClient(kgo.SeedBrokers(f.Broker))
	if e != nil {
		return 0, e
	}
	defer c.Close()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	topic := kmsg.NewOffsetFetchRequestTopic()
	topic.Topic = f.Prefix + "-delivery-requests"
	topic.Partitions = []int32{partition}
	req.Topics = []kmsg.OffsetFetchRequestTopic{topic}
	res, e := req.RequestWith(ctx, c)
	if e != nil {
		return 0, e
	}
	if len(res.Topics) != 1 || len(res.Topics[0].Partitions) != 1 || res.Topics[0].Partitions[0].ErrorCode != 0 {
		return 0, fmt.Errorf("broker offset unavailable")
	}
	return res.Topics[0].Partitions[0].Offset, nil
}
