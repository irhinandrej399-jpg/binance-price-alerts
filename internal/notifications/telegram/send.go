// Package telegram implements one sendMessage attempt without scheduling retries.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Kind string

const (
	NotSent     Kind = "not_sent"
	RateLimited Kind = "rate_limited"
	Forbidden   Kind = "forbidden"
	Server      Kind = "server"
	Rejected    Kind = "rejected"
	Unknown     Kind = "unknown"
)

// Error contains only safe diagnostics. Unknown means delivery may have happened.
// RetryAfterSeconds is Telegram's delay, not a locally scheduled retry.
// Timeout distinguishes a deadline from other ambiguous transport/protocol errors.
type Error struct {
	Kind              Kind
	HTTPStatus        int
	Code              int
	RetryAfterSeconds int64
	Timeout           bool
}

func (e *Error) Error() string { return "telegram sendMessage: " + string(e.Kind) }

// Config requires a positive timeout covering the entire request and response.
// BaseURL defaults to Telegram; HTTP is accepted only for loopback test servers.
type Config struct {
	Token   string
	Timeout time.Duration
	BaseURL string
}

type Client struct {
	endpoint string
	http     *http.Client
}

func New(cfg Config) (*Client, error) {
	invalid := errors.New("invalid Telegram sender configuration")
	if cfg.Token == "" || cfg.Timeout <= 0 {
		return nil, invalid
	}
	for _, r := range cfg.Token {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == ':' || r == '_' || r == '-') {
			return nil, invalid
		}
	}
	base := cfg.BaseURL
	if base == "" {
		base = "https://api.telegram.org"
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return nil, invalid
	}
	loopback := net.ParseIP(u.Hostname())
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback != nil && loopback.IsLoopback()) {
		return nil, invalid
	}
	endpoint := strings.TrimRight(u.String(), "/") + "/bot" + cfg.Token + "/sendMessage"
	return &Client{endpoint: endpoint, http: &http.Client{Timeout: cfg.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Send uses the private chat and HTML text from the caller's current attempt
// admission. It never follows redirects or retries the POST. The caller owns
// admission freshness, limiters, persistence and all subsequent retry decisions.
func (c *Client) Send(ctx context.Context, chatID int64, text string) (int64, error) {
	if chatID <= 0 || strings.TrimSpace(text) == "" || ctx.Err() != nil {
		return 0, &Error{Kind: NotSent}
	}
	body, _ := json.Marshal(struct {
		ChatID    int64  `json:"chat_id"`
		Text      string `json:"text"`
		ParseMode string `json:"parse_mode"`
	}{chatID, text, "HTML"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, &Error{Kind: NotSent}
	}
	// Disable body replay even when the transport considers a connection failure safe.
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, unknownError(err, 0)
	} // Never expose url.Error: its URL contains the bot token.
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if readErr != nil {
		return 0, unknownError(readErr, resp.StatusCode)
	}
	var envelope struct {
		OK     *bool `json:"ok"`
		Code   int   `json:"error_code"`
		Result struct {
			ID int64 `json:"message_id"`
		} `json:"result"`
		Parameters struct {
			Retry int64 `json:"retry_after"`
		} `json:"parameters"`
	}
	valid := len(raw) <= 1<<20 && json.Unmarshal(raw, &envelope) == nil
	// HTTP errors remain classified even if an upstream proxy supplied non-JSON.
	code := resp.StatusCode
	if code >= 200 && code < 300 {
		if valid && envelope.OK != nil && *envelope.OK && envelope.Result.ID > 0 {
			return envelope.Result.ID, nil
		}
		if !valid || envelope.OK == nil || *envelope.OK || envelope.Code < 400 || envelope.Code > 599 {
			return 0, &Error{Kind: Unknown, HTTPStatus: resp.StatusCode}
		}
		code = envelope.Code
	}
	e := &Error{Kind: Unknown, HTTPStatus: resp.StatusCode}
	if valid {
		e.Code = envelope.Code
	}
	switch {
	case code == 429:
		e.Kind = RateLimited
		if valid && envelope.Parameters.Retry > 0 {
			e.RetryAfterSeconds = envelope.Parameters.Retry
		}
	case code == 403:
		e.Kind = Forbidden
	case code >= 500 && code <= 599:
		e.Kind = Server
	case code >= 400 && code <= 499:
		e.Kind = Rejected
	}
	return 0, e
}
func unknownError(err error, status int) *Error {
	var n net.Error
	return &Error{Kind: Unknown, HTTPStatus: status, Timeout: errors.Is(err, context.DeadlineExceeded) || errors.As(err, &n) && n.Timeout()}
}
