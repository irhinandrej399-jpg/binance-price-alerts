package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	rpcAddr, rpcCert, rpcKey, cleanerURI               string
	addr, dsn, prices, changes, environment, group, id string
	api, latest, serverName, ca, cert, key             string
	pricesSource, changesSource                        kafkasource.Identity
	brokers                                            []string
	bootstrap                                          map[int32]int64
	snapshotTTL, replayTTL, headInterval               time.Duration
	snapshotPage, replayPage                           int32
}

var errConfig = errors.New("matcher: invalid configuration")
var name = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,249}$`)

func addressOK(s string) bool {
	host, port, e := net.SplitHostPort(s)
	n, e2 := strconv.Atoi(port)
	return e == nil && e2 == nil && host != "" && n > 0 && n <= 65535
}
func loadConfig(get func(string) string) (config, error) {
	c := config{rpcAddr: get("MATCHER_RPC_ADDR"), rpcCert: get("MATCHER_RPC_CERT_FILE"), rpcKey: get("MATCHER_RPC_KEY_FILE"), cleanerURI: get("CLEANER_SERVICE_URI"), addr: get("HTTP_ADDR"), dsn: get("MATCHER_DATABASE_URL"), prices: get("PRICES_TOPIC"), changes: get("SUBSCRIPTION_CHANGES_TOPIC"), environment: get("MATCHER_ENVIRONMENT"), group: get("MATCHER_GROUP"), id: get("MATCHER_REPLICA_ID"), api: get("API_RPC_ADDR"), latest: get("LATEST_RPC_ADDR"), serverName: get("RPC_SERVER_NAME"), ca: get("RPC_CA_FILE"), cert: get("RPC_CERT_FILE"), key: get("RPC_KEY_FILE")}
	for key, target := range map[string]*kafkasource.Identity{"PRICES_TOPIC_ID": &c.pricesSource, "SUBSCRIPTION_CHANGES_TOPIC_ID": &c.changesSource} {
		b, err := hex.DecodeString(get(key))
		if err != nil || len(b) != 16 {
			return config{}, errConfig
		}
		target.ClusterID = get("KAFKA_CLUSTER_ID")
		copy(target.TopicID[:], b)
		target.Topic = c.prices
		if key == "SUBSCRIPTION_CHANGES_TOPIC_ID" {
			target.Topic = c.changes
		}
		if target.Validate() != nil {
			return config{}, errConfig
		}
	}
	for _, s := range []string{c.addr, c.api, c.latest, c.rpcAddr} {
		if !addressOK(s) {
			return config{}, errConfig
		}
	}
	db, e := pgxpool.ParseConfig(c.dsn)
	if c.dsn == "" || e != nil || db.ConnConfig.User != "matcher_runtime" || db.ConnConfig.Database != "matcher" {
		return config{}, errConfig
	}
	for _, s := range []string{c.prices, c.changes, c.environment, c.group, c.id} {
		if !name.MatchString(s) || s == "." || s == ".." {
			return config{}, errConfig
		}
	}
	if c.prices == c.changes {
		return config{}, errConfig
	}
	for _, s := range []string{c.serverName, c.ca, c.cert, c.key, c.rpcCert, c.rpcKey} {
		if strings.TrimSpace(s) == "" {
			return config{}, errConfig
		}
	}
	u, err := url.Parse(c.cleanerURI)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return config{}, errConfig
	}
	c.brokers = strings.Split(get("KAFKA_BROKERS"), ",")
	for _, s := range c.brokers {
		if !addressOK(s) {
			return config{}, errConfig
		}
	}
	for k, d := range map[string]*time.Duration{"SNAPSHOT_TTL": &c.snapshotTTL, "REPLAY_TTL": &c.replayTTL, "HEAD_INTERVAL": &c.headInterval} {
		v, e := time.ParseDuration(get(k))
		if e != nil || v < time.Millisecond || v > 24*time.Hour || v%time.Millisecond != 0 {
			return config{}, errConfig
		}
		*d = v
	}
	// Observe the fixed pin before its expiry; this does not extend TTL.
	if c.headInterval >= c.replayTTL {
		return config{}, errConfig
	}
	for k, d := range map[string]*int32{"SNAPSHOT_PAGE_SIZE": &c.snapshotPage, "REPLAY_PAGE_SIZE": &c.replayPage} {
		v, e := strconv.Atoi(get(k))
		if e != nil || v < 1 || v > 1000 {
			return config{}, errConfig
		}
		*d = int32(v)
	}
	// Decode tokens to reject duplicate partition keys as well as missing keys.
	dec := json.NewDecoder(strings.NewReader(get("PRICES_BOOTSTRAP")))
	token, e := dec.Token()
	if e != nil || token != json.Delim('{') {
		return config{}, errConfig
	}
	c.bootstrap = map[int32]int64{}
	for dec.More() {
		k, e := dec.Token()
		if e != nil {
			return config{}, errConfig
		}
		s, ok := k.(string)
		if !ok {
			return config{}, errConfig
		}
		p, e := strconv.Atoi(s)
		if e != nil || p < 0 || p >= 16 || strconv.Itoa(p) != s {
			return config{}, errConfig
		}
		if _, ok := c.bootstrap[int32(p)]; ok {
			return config{}, errConfig
		}
		var o *int64
		if dec.Decode(&o) != nil || o == nil || *o < 0 {
			return config{}, errConfig
		}
		c.bootstrap[int32(p)] = *o
	}
	if _, e = dec.Token(); e != nil || len(c.bootstrap) != 16 {
		return config{}, errConfig
	}
	if _, e = dec.Token(); e != io.EOF {
		return config{}, errConfig
	}
	return c, nil
}
