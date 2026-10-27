// Package catalog fetches the currently tradable Binance Spot USDT pairs.
package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Pair retains Binance's exact symbol and assets; symbols need not be ASCII.
type Pair struct {
	Symbol     string
	BaseAsset  string
	QuoteAsset string
}

// Snapshot is a complete successful fetch, with the local completion time in UTC.
// UpdatedAt is not Binance serverTime or a trade timestamp.
type Snapshot struct {
	Pairs     []Pair
	UpdatedAt time.Time
}

// Client has no cache or refresh loop and is safe for concurrent use if now is.
type Client struct {
	httpClient *http.Client
	endpoint   string
	now        func() time.Time
}

// New requires an HTTP client and clock. baseURL is the Binance origin, e.g.
// https://data-api.binance.vision. The caller owns timeouts and retry policy.
func New(httpClient *http.Client, baseURL string, now func() time.Time) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("catalog: invalid Binance origin")
	}
	if httpClient == nil || now == nil {
		return nil, errors.New("catalog: HTTP client and clock required")
	}
	u.Path = "/api/v3/exchangeInfo"
	return &Client{httpClient: httpClient, endpoint: u.String(), now: now}, nil
}

// Fetch makes one public request and filters locally. Every failure returns a
// zero snapshot, so callers cannot mistake a failed refresh for an empty market.
func (c *Client) Fetch(ctx context.Context) (Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint, nil)
	if err != nil {
		return Snapshot{}, fmt.Errorf("catalog: create request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Snapshot{}, fmt.Errorf("catalog: request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Snapshot{}, fmt.Errorf("catalog: HTTP status %d", resp.StatusCode)
	}
	const maxResponseBytes = 16 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return Snapshot{}, fmt.Errorf("catalog: read response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return Snapshot{}, errors.New("catalog: response exceeds 16 MiB")
	}
	var payload struct {
		Code    *int `json:"code"`
		Symbols []struct {
			Symbol     string `json:"symbol"`
			BaseAsset  string `json:"baseAsset"`
			QuoteAsset string `json:"quoteAsset"`
			Status     string `json:"status"`
			Spot       bool   `json:"isSpotTradingAllowed"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return Snapshot{}, fmt.Errorf("catalog: decode response: %w", err)
	}
	if payload.Code != nil || payload.Symbols == nil {
		return Snapshot{}, errors.New("catalog: API error or missing symbols")
	}
	pairs := make([]Pair, 0, len(payload.Symbols))
	for _, s := range payload.Symbols {
		if s.QuoteAsset != "USDT" || s.Status != "TRADING" || !s.Spot {
			continue
		}
		if s.Symbol == "" || s.BaseAsset == "" {
			return Snapshot{}, errors.New("catalog: invalid pair")
		}
		pairs = append(pairs, Pair{Symbol: s.Symbol, BaseAsset: s.BaseAsset, QuoteAsset: s.QuoteAsset})
	}
	return Snapshot{Pairs: pairs, UpdatedAt: c.now().UTC()}, nil
}
