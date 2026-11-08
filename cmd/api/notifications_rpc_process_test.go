package main

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func testNotificationsRPCProcess(t *testing.T, ctx context.Context, cfg apiprocess.Config, pool *pgxpool.Pool, ca authority, addr string, quotes *processQuotes) {
	t.Helper()
	t.Log("notifications RPC runtime, canonical replay and pending result recovery")
	conn := rpcConn(t, ca, addr, cfg.Env["NOTIFICATIONS_SERVICE_URI"])
	matcher := rpcConn(t, ca, addr, cfg.Env["MATCHER_SERVICE_URI"])
	stranger := rpcConn(t, ca, addr, "spiffe://local/stranger")
	admission := pb.NewAdmissionClient(conn)
	results := pb.NewDeliveryResultsClient(conn)
	f, err := seedDrainFixture(ctx, pool)
	if err != nil {
		t.Fatal("notifications seed")
	}
	prepare := &pb.PrepareRecipientRequest{OwnerId: f.OwnerID, SubscriptionId: f.Delivery.ID, WorkflowId: f.Delivery.Workflow.TriggerID}
	request := &pb.AdmitDeliveryRequest{OwnerId: f.OwnerID, SubscriptionId: f.Delivery.ID, WorkflowId: prepare.WorkflowId, RequestId: "ops-071-canonical"}
	var permit *pb.AdmitDeliveryResponse
	var report *pb.SaveDeliveryResultRequest
	var admissionBytes []byte
	for restart := 0; restart < 2; restart++ {
		err = withDrainedAPI(ctx, cfg, pool, func(p *apiprocess.Process) error {
			call, cancel := context.WithTimeout(ctx, 12*time.Second)
			defer cancel()
			recipient, e := admission.PrepareRecipient(call, prepare)
			if e != nil {
				return fmt.Errorf("PrepareRecipient runtime: %w", e)
			}
			if recipient.Outcome != "recipient_ready" || recipient.Recipient == nil {
				return errors.New("recipient missing")
			}
			request.ExpectedRecipient = recipient.Recipient
			for _, denied := range []*pb.AdmitDeliveryRequest{
				{OwnerId: f.OwnerID, SubscriptionId: f.Delivery.ID, WorkflowId: f.Active.ID, RequestId: "wrong-workflow"},
				{OwnerId: f.Active.ID, SubscriptionId: f.Delivery.ID, WorkflowId: prepare.WorkflowId, RequestId: "wrong-owner"},
			} {
				_, e = admission.AdmitDelivery(call, denied)
				want := codes.FailedPrecondition
				if denied.OwnerId != f.OwnerID {
					want = codes.NotFound
				}
				if status.Code(e) != want {
					return fmt.Errorf("identity tuple check: %v", e)
				}
			}
			for _, deniedConn := range []pb.AdmissionClient{pb.NewAdmissionClient(matcher), pb.NewAdmissionClient(stranger)} {
				if _, e = deniedConn.PrepareRecipient(call, prepare); status.Code(e) != codes.PermissionDenied {
					return errors.New("prepare policy")
				}
				if _, e = deniedConn.AdmitDelivery(call, request); status.Code(e) != codes.PermissionDenied {
					return errors.New("admission policy")
				}
			}
			// notifications must retain none of the matcher's methods on the shared listener.
			for _, method := range []string{pb.SubscriptionSnapshots_BuildSnapshot_FullMethodName, pb.SubscriptionSnapshots_ReadSnapshotPage_FullMethodName, pb.SubscriptionReplay_BeginReplay_FullMethodName, pb.SubscriptionReplay_ReadReplayPage_FullMethodName, pb.SubscriptionReplay_AcknowledgeReplay_FullMethodName, pb.SubscriptionReplay_AbandonReplay_FullMethodName, pb.Delivery_RequestDelivery_FullMethodName, "/api.v1.Unknown/Call"} {
				if e = conn.Invoke(call, method, &pb.BuildSnapshotRequest{}, &pb.SnapshotPageResponse{}); status.Code(e) != codes.PermissionDenied {
					return fmt.Errorf("notifications allowed %s", method)
				}
			}
			if restart == 0 {
				var count int
				if e = pool.QueryRow(call, `SELECT count(*) FROM app.delivery_admissions WHERE subscription_id=$1`, f.Delivery.ID).Scan(&count); e != nil || count != 0 {
					return errors.New("prepare mutated admission")
				}
				quotes.mode.Store(1)
				if _, e = admission.AdmitDelivery(call, request); status.Code(e) != codes.Unavailable {
					return fmt.Errorf("Latest failure: %v", e)
				}
				// CAS rejects an un-limited address before any attempt is created.
				bad := proto.Clone(request).(*pb.AdmitDeliveryRequest)
				bad.RequestId = "ops-071-cas"
				bad.ExpectedRecipient.ChatId++
				if _, e = admission.AdmitDelivery(call, bad); status.Code(e) != codes.Aborted {
					return fmt.Errorf("recipient CAS: %v", e)
				}
				quotes.mode.Store(2)
				short, stop := context.WithTimeout(call, 200*time.Millisecond)
				_, e = admission.AdmitDelivery(short, request)
				stop()
				if status.Code(e) != codes.DeadlineExceeded {
					return fmt.Errorf("Latest cancellation: %v", e)
				}
				select {
				case <-quotes.entered:
				case <-call.Done():
					return errors.New("Latest not entered")
				}
				select {
				case <-quotes.canceled:
				case <-call.Done():
					return errors.New("Latest not canceled")
				}
				if e = pool.QueryRow(call, `SELECT count(*) FROM app.delivery_admissions WHERE subscription_id=$1`, f.Delivery.ID).Scan(&count); e != nil || count != 0 {
					return errors.New("failed Latest/CAS created attempt")
				}
				quotes.mode.Store(0)
			} else {
				quotes.mode.Store(1)
			} // canonical replay never calls Latest
			got, e := admission.AdmitDelivery(call, request)
			if e != nil || got.Outcome != "admitted" || got.Attempt == nil {
				return fmt.Errorf("admission runtime: %v", e)
			}
			if got.Attempt.ChatId != recipient.Recipient.ChatId || got.Attempt.BindingVersion != recipient.Recipient.BindingVersion || got.Attempt.Quote.Price != "101.000000000000000001" {
				return errors.New("permit address/quote mismatch")
			}
			var body []byte
			var attempt string
			if e = pool.QueryRow(call, `SELECT response,attempt_id FROM app.delivery_admissions WHERE subscription_id=$1 AND request_id=$2`, f.Delivery.ID, request.RequestId).Scan(&body, &attempt); e != nil || attempt != got.Attempt.AttemptId {
				return errors.New("permit not committed before response")
			}
			if restart == 0 {
				permit = got
				admissionBytes = body
				report = &pb.SaveDeliveryResultRequest{OwnerId: f.OwnerID, SubscriptionId: f.Delivery.ID, WorkflowId: prepare.WorkflowId, AttemptId: got.Attempt.AttemptId, Outcome: "succeeded", MessageId: 71001}
			} else if !proto.Equal(permit, got) || string(body) != string(admissionBytes) {
				return errors.New("restart changed admission bytes/IDs/deadline")
			}
			for _, denied := range []pb.DeliveryResultsClient{pb.NewDeliveryResultsClient(matcher), pb.NewDeliveryResultsClient(stranger)} {
				if _, e = denied.SaveDeliveryResult(call, report); status.Code(e) != codes.PermissionDenied {
					return errors.New("result policy")
				}
			}

			if restart == 0 {
				lock, e := pool.Begin(call)
				if e != nil {
					return e
				}
				defer lock.Rollback(context.Background())
				if _, e = lock.Exec(call, `SELECT id FROM app.users WHERE id=$1 FOR UPDATE`, f.OwnerID); e != nil {
					return e
				}
				short, stop := context.WithTimeout(call, time.Second)
				pending := make(chan error, 1)
				go func() { _, e := results.SaveDeliveryResult(short, report); pending <- e }()
				if e = eventually(short, func() bool {
					var waiting bool
					e := pool.QueryRow(short, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%app.users%FOR UPDATE%')`).Scan(&waiting)
					return e == nil && waiting
				}); e != nil {
					stop()
					return errors.New("result did not reach SQL lock")
				}
				stop()
				if e = <-pending; status.Code(e) != codes.Canceled && status.Code(e) != codes.DeadlineExceeded {
					return fmt.Errorf("pending report cancellation: %v", e)
				}
				// A client cancellation can return before the server observes it.
				// Keep the SQL lock until its waiter has actually left.
				if e = eventually(call, func() bool {
					var waiting bool
					e := pool.QueryRow(call, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%app.users%FOR UPDATE%')`).Scan(&waiting)
					return e == nil && !waiting
				}); e != nil {
					return errors.New("canceled result SQL waiter leaked")
				}
				if e = lock.Rollback(call); e != nil {
					return e
				}
				var count int
				if e = pool.QueryRow(call, `SELECT count(*) FROM app.delivery_results WHERE attempt_id=$1`, report.AttemptId).Scan(&count); e != nil || count != 0 {
					return errors.New("canceled pending report committed")
				}
			}
			if restart == 1 {
				// The same locally retained pending report survives API restart. No new
				// admission/attempt or external Telegram call is needed to acknowledge it.
				var version int64
				for repeat := 0; repeat < 2; repeat++ {
					saved, e := results.SaveDeliveryResult(call, report)
					if e != nil || saved.State != "delivered" {
						return fmt.Errorf("pending report: %v", e)
					}
					if repeat == 0 {
						version = saved.SubscriptionVersion
					} else if saved.SubscriptionVersion != version {
						return errors.New("duplicate report transition")
					}
				}
				var count int
				var message int64
				if e = pool.QueryRow(call, `SELECT count(*) FROM app.delivery_results WHERE attempt_id=$1 AND outcome='succeeded'`, report.AttemptId).Scan(&count); e != nil || count != 1 {
					return errors.New("result not durable/idempotent")
				}
				if e = pool.QueryRow(call, `SELECT delivered_message_id FROM app.subscriptions WHERE id=$1 AND state='delivered'`, f.Delivery.ID).Scan(&message); e != nil || message != report.MessageId {
					return errors.New("canonical success missing")
				}
				conflict := proto.Clone(report).(*pb.SaveDeliveryResultRequest)
				conflict.MessageId++
				if _, e = results.SaveDeliveryResult(call, conflict); status.Code(e) != codes.AlreadyExists {
					return errors.New("conflicting result accepted")
				}
				replay, e := admission.AdmitDelivery(call, request)
				if e != nil || !proto.Equal(replay, permit) {
					return errors.New("terminal changed historical admission")
				}
			}
			// Cancel an in-flight nested mTLS Latest on SIGTERM. Observe upstream
			// cancellation before accepting process exit and cleanup.
			quotes.mode.Store(2)
			other, e := seedDrainFixture(call, pool)
			if e != nil {
				return errors.New("shutdown seed")
			}
			held := make(chan error, 1)
			go func() {
				_, e := admission.AdmitDelivery(call, &pb.AdmitDeliveryRequest{OwnerId: other.OwnerID, SubscriptionId: other.Delivery.ID, WorkflowId: other.Delivery.Workflow.TriggerID, RequestId: "ops-071-held"})
				held <- e
			}()
			select {
			case <-quotes.entered:
			case <-call.Done():
				return errors.New("held Latest not entered")
			}
			if e = syscall.Kill(p.PID(), syscall.SIGTERM); e != nil {
				return e
			}
			select {
			case <-quotes.canceled:
			case <-call.Done():
				return errors.New("shutdown did not cancel Latest")
			}
			select {
			case e = <-held:
				if e == nil {
					return errors.New("held admission succeeded")
				}
			case <-call.Done():
				return errors.New("held admission leaked")
			}
			select {
			case <-p.Done():
			case <-call.Done():
				return errors.New("notifications shutdown exceeded bound")
			}
			if code, ok := p.ExitCode(); !ok || code != 0 {
				return errors.New("notifications shutdown not graceful")
			}
			var count int
			if e = pool.QueryRow(call, `SELECT count(*) FROM app.delivery_admissions WHERE subscription_id=$1`, other.Delivery.ID).Scan(&count); e != nil || count != 0 {
				return errors.New("shutdown created attempt")
			}
			return nil
		})
		quotes.mode.Store(0)
		if err != nil {
			t.Fatal(err)
		}
	}
}
