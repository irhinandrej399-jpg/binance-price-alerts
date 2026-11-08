package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/telegramfake"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testSubscriptionsProcess(t *testing.T, ctx context.Context, cfg apiprocess.Config, pool *pgxpool.Pool, quotes *processQuotes) {
	t.Helper()
	t.Log("subscriptions HTTP runtime, isolation, journal, RPC failure and drain")
	const input = `{"pair":"BTCUSDT","price":"102.000000000000000001","direction":"up"}`
	var owner, token, other, id string
	var canonical []byte
	request := func(p *apiprocess.Process, method, path, body, bearer string, want int, dst any) error {
		req, e := http.NewRequestWithContext(ctx, method, p.URL+path, strings.NewReader(body))
		if e != nil {
			return e
		}
		req.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		resp, e := (&http.Client{Timeout: 9 * time.Second}).Do(req)
		if e != nil {
			return errors.New("subscription HTTP failed")
		}
		defer resp.Body.Close()
		if resp.StatusCode != want {
			return fmt.Errorf("%s %s status %d want %d", method, path, resp.StatusCode, want)
		}
		if dst != nil {
			return json.NewDecoder(resp.Body).Decode(dst)
		}
		_, e = io.Copy(io.Discard, resp.Body)
		return e
	}
	checkSQL := func(count int) error {
		var slots, records int
		e := pool.QueryRow(ctx, `SELECT active_subscriptions,(SELECT count(*) FROM app.subscriptions WHERE owner_id=$1) FROM app.users WHERE id=$1`, owner).Scan(&slots, &records)
		// Journal bytes are checked independently below; count records/slots here.
		if e != nil || slots != count || records != count {
			return errors.New("subscription/quota not atomic")
		}
		return nil
	}
	list := func(p *apiprocess.Process, bearer string, want int) error {
		var got struct {
			Subscriptions []struct {
				ID        string `json:"id"`
				Threshold string `json:"threshold"`
			} `json:"subscriptions"`
		}
		if e := request(p, "GET", "/v1/subscriptions", "", bearer, 200, &got); e != nil {
			return e
		}
		if len(got.Subscriptions) != want {
			return errors.New("owner list leaked/missing subscriptions")
		}
		if want == 1 && (got.Subscriptions[0].ID != id || got.Subscriptions[0].Threshold != "102.000000000000000001") {
			return errors.New("list changed identity/price")
		}
		return nil
	}
	err := withDrainedAPI(ctx, cfg, pool, func(p *apiprocess.Process) error {
		for _, method := range []string{"POST", "GET"} {
			for _, bearer := range []string{"", "invalid"} {
				if e := request(p, method, "/v1/subscriptions", input, bearer, 401, nil); e != nil {
					return e
				}
			}
		}
		for i := 0; i < 2; i++ {
			credentials := fmt.Sprintf(`{"email":"ops073-%d-%d@example.test","password":"test-only-password"}`, time.Now().UnixNano(), i)
			var account struct {
				ID string `json:"id"`
			}
			var session struct {
				Token string `json:"access_token"`
			}
			if e := request(p, "POST", "/v1/accounts", credentials, "", 201, &account); e != nil {
				return e
			}
			if e := request(p, "POST", "/v1/sessions", credentials, "", 200, &session); e != nil {
				return e
			}
			if i == 0 {
				owner, token = account.ID, session.Token
			} else {
				other = session.Token
			}
		}
		if e := list(p, token, 0); e != nil {
			return e
		}
		if e := request(p, "POST", "/v1/subscriptions", input, token, 409, nil); e != nil {
			return e
		}
		if e := checkSQL(0); e != nil {
			return e
		}
		var code struct {
			Code string `json:"code"`
		}
		if e := request(p, "POST", "/v1/telegram/link-codes", "", token, 201, &code); e != nil {
			return e
		}
		if status, e := telegramfake.Link(ctx, p.URL, 73001, 73001, code.Code); e != nil || status != 200 {
			return errors.New("subscription link failed")
		}
		if e := request(p, "POST", "/v1/subscriptions", strings.Repeat("x", 65537), token, 413, nil); e != nil {
			return e
		}
		if e := request(p, "POST", "/v1/subscriptions", strings.TrimSuffix(input, "}")+`,"user_id":"`+owner+`"}`, token, 400, nil); e != nil {
			return e
		}
		if e := request(p, "POST", "/v1/subscriptions", strings.ReplaceAll(input, "BTCUSDT", "UNKNOWNUSDT"), token, 400, nil); e != nil {
			return e
		}
		quotes.catalogMode.Store(1)
		var failure struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if e := request(p, "POST", "/v1/subscriptions", input, token, 503, &failure); e != nil {
			return e
		}
		if failure.Error.Code != "catalog_unavailable" {
			return errors.New("catalog error changed")
		}
		quotes.catalogMode.Store(0)
		quotes.mode.Store(1)
		if e := request(p, "POST", "/v1/subscriptions", input, token, 503, &failure); e != nil {
			return e
		}
		if failure.Error.Code != "quote_unavailable" {
			return errors.New("quote error changed")
		}
		quotes.mode.Store(0)
		if e := checkSQL(0); e != nil {
			return e
		}
		var created struct {
			ID            string   `json:"id"`
			CreationPrice string   `json:"creation_price"`
			Warnings      []string `json:"warnings"`
		}
		if e := request(p, "POST", "/v1/subscriptions", input, token, 201, &created); e != nil {
			return e
		}
		id = created.ID
		if id == "" || created.CreationPrice != "101.000000000000000001" || len(created.Warnings) != 1 || created.Warnings[0] != "stream_unconfirmed" {
			return errors.New("creation response changed")
		}
		if e := checkSQL(1); e != nil {
			return e
		}
		var threshold, snapshot string
		var reserved bool
		if e := pool.QueryRow(ctx, `SELECT threshold,creation_price,quota_reserved FROM app.subscriptions WHERE id=$1 AND owner_id=$2`, id, owner).Scan(&threshold, &snapshot, &reserved); e != nil || threshold != "102.000000000000000001" || snapshot != created.CreationPrice || !reserved {
			return errors.New("SQL snapshot/threshold mismatch")
		}
		if e := pool.QueryRow(ctx, `SELECT body FROM app.api_journal ORDER BY position DESC LIMIT 1`).Scan(&canonical); e != nil {
			return e
		}
		_, event, e := changed.Decode(canonical)
		if e != nil || event.SubscriptionID != id || event.OwnerID != owner || event.State != "active" || event.Threshold.String() != threshold || event.CreationSnapshot.Price.String() != snapshot {
			return errors.New("creation journal mismatch")
		}
		if e = consumeCopies(ctx, cfg, id, canonical, 1); e != nil {
			return e
		}
		if e = list(p, token, 1); e != nil {
			return e
		}
		if e = list(p, other, 0); e != nil {
			return e
		}
		if e = request(p, "DELETE", "/v1/subscriptions/"+id, "", other, 404, nil); e != nil {
			return e
		}
		// Catalog's finite RPC budget ends an accepted HTTP request during drain.
		quotes.catalogMode.Store(2)
		pending := make(chan error, 1)
		go func() { pending <- request(p, "POST", "/v1/subscriptions", input, token, 503, nil) }()
		select {
		case <-quotes.entered:
		case <-ctx.Done():
			return ctx.Err()
		}
		if e = syscall.Kill(p.PID(), syscall.SIGTERM); e != nil {
			return e
		}
		select {
		case <-quotes.canceled:
		case <-ctx.Done():
			return ctx.Err()
		}
		if e = <-pending; e != nil {
			return e
		}
		select {
		case <-p.Done():
		case <-time.After(9 * time.Second):
			return errors.New("subscription drain timed out")
		}
		if exit, ok := p.ExitCode(); !ok || exit != 0 {
			return errors.New("subscription drain failed")
		}
		return checkSQL(1)
	})
	quotes.mode.Store(0)
	quotes.catalogMode.Store(0)
	if err != nil {
		t.Fatal(err)
	}
	err = withDrainedAPI(ctx, cfg, pool, func(p *apiprocess.Process) error {
		if e := list(p, token, 1); e != nil {
			return e
		}
		// List reads only the API database, including while market-data is unavailable.
		quotes.catalogMode.Store(1)
		quotes.mode.Store(1)
		if e := list(p, token, 1); e != nil {
			return e
		}
		if e := request(p, "DELETE", "/v1/subscriptions/"+id, "", token, 200, nil); e != nil {
			return e
		}
		if e := list(p, token, 0); e != nil {
			return e
		}
		var slots int
		if e := pool.QueryRow(ctx, `SELECT active_subscriptions FROM app.users WHERE id=$1`, owner).Scan(&slots); e != nil || slots != 0 {
			return errors.New("cancel retained slot")
		}
		var body []byte
		if e := pool.QueryRow(ctx, `SELECT body FROM app.api_journal ORDER BY position DESC LIMIT 1`).Scan(&body); e != nil {
			return e
		}
		_, event, e := changed.Decode(body)
		if e != nil || event.SubscriptionID != id || event.State != "cancelled" {
			return errors.New("cancel journal mismatch")
		}
		return consumeCopies(ctx, cfg, id, body, 1)
	})
	quotes.mode.Store(0)
	quotes.catalogMode.Store(0)
	if err != nil {
		t.Fatal(err)
	}
}
