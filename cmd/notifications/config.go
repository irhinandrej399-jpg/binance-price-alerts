package main

import (
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	binding                                                                   intake.Binding
	rpcAddr, rpcCert, rpcKey, cleanerURI                                      string
	addr, dsn, topic, group, api, serverName, identity, ca, cert, key, botID  string
	brokers                                                                   []string
	telegram                                                                  telegram.Config
	retry                                                                     retry.Config
	botRate, concurrency, reportLimit                                         int
	chatInterval, leaseTTL, recoveryInterval, interval, timeout, writeTimeout time.Duration
}

var errConfig = errors.New("notifications: invalid configuration")
var name = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,249}$`)

func addressOK(s string) bool {
	h, p, e := net.SplitHostPort(s)
	n, e2 := strconv.Atoi(p)
	return e == nil && e2 == nil && h != "" && n > 0 && n <= 65535
}
func loadConfig(get func(string) string) (config, error) {
	c := config{addr: get("HTTP_ADDR"), dsn: get("NOTIFICATIONS_DATABASE_URL"), topic: get("DELIVERY_REQUESTS_TOPIC"), group: get("NOTIFICATIONS_GROUP"), api: get("API_RPC_ADDR"), serverName: get("RPC_SERVER_NAME"), identity: get("RPC_IDENTITY"), ca: get("RPC_CA_FILE"), cert: get("RPC_CERT_FILE"), key: get("RPC_KEY_FILE")}
	c.rpcAddr, c.rpcCert, c.rpcKey, c.cleanerURI = get("RPC_ADDR"), get("RPC_SERVER_CERT_FILE"), get("RPC_SERVER_KEY_FILE"), get("CLEANER_SERVICE_URI")
	c.binding.ConsumerID = get("NOTIFICATIONS_CONSUMER_ID")
	c.binding.Generation = get("NOTIFICATIONS_GENERATION")
	c.binding.Source.ClusterID = get("KAFKA_CLUSTER_ID")
	c.binding.Source.Topic = c.topic
	tid, err := hex.DecodeString(get("DELIVERY_REQUESTS_TOPIC_ID"))
	if err != nil || len(tid) != 16 {
		return config{}, errConfig
	}
	copy(c.binding.Source.TopicID[:], tid)
	if c.binding.Validate() != nil {
		return config{}, errConfig
	}
	for _, s := range []string{c.addr, c.api, c.rpcAddr} {
		if !addressOK(s) {
			return config{}, errConfig
		}
	}
	db, e := pgxpool.ParseConfig(c.dsn)
	if c.dsn == "" || e != nil || db.ConnConfig.User != "notifications_runtime" || db.ConnConfig.Database != "notifications" {
		return config{}, errConfig
	}
	for _, s := range []string{c.topic, c.group} {
		if !name.MatchString(s) || s == "." || s == ".." {
			return config{}, errConfig
		}
	}
	for _, s := range []string{c.serverName, c.ca, c.cert, c.key, c.rpcCert, c.rpcKey} {
		if strings.TrimSpace(s) == "" {
			return config{}, errConfig
		}
	}
	id, e := url.Parse(c.identity)
	if e != nil || id.Scheme != "spiffe" || id.Host == "" || id.User != nil || id.RawQuery != "" || id.Fragment != "" || id.Path == "" {
		return config{}, errConfig
	}
	cleaner, err := url.Parse(c.cleanerURI)
	if err != nil || cleaner.Scheme != "spiffe" || cleaner.Host == "" || cleaner.Path == "" || cleaner.User != nil || cleaner.RawQuery != "" || cleaner.Fragment != "" {
		return config{}, errConfig
	}
	c.brokers = strings.Split(get("KAFKA_BROKERS"), ",")
	for _, s := range c.brokers {
		if !addressOK(s) {
			return config{}, errConfig
		}
	}
	for k, d := range map[string]*time.Duration{"TELEGRAM_TIMEOUT": &c.telegram.Timeout, "RETRY_BASE_DELAY": &c.retry.BaseDelay, "RETRY_MAX_DELAY": &c.retry.MaxDelay, "CONDITION_INTERVAL": &c.retry.ConditionInterval, "CHAT_INTERVAL": &c.chatInterval, "LEASE_TTL": &c.leaseTTL, "RECOVERY_INTERVAL": &c.recoveryInterval, "WORKER_INTERVAL": &c.interval, "WORKER_TIMEOUT": &c.timeout, "WRITE_TIMEOUT": &c.writeTimeout} {
		v, e := time.ParseDuration(get(k))
		if e != nil || v < time.Microsecond || v > time.Hour || v%time.Microsecond != 0 {
			return config{}, errConfig
		}
		*d = v
	}
	for k, d := range map[string]*int{"BOT_RATE": &c.botRate, "WORKER_CONCURRENCY": &c.concurrency, "REPORT_LIMIT": &c.reportLimit} {
		v, e := strconv.Atoi(get(k))
		max := 1000
		if k == "BOT_RATE" {
			max = 1000000
		}
		if e != nil || v < 1 || v > max {
			return config{}, errConfig
		}
		*d = v
	}
	failures, e := strconv.ParseUint(get("MAX_FAILURES"), 10, 32)
	if e != nil || failures == 0 {
		return config{}, errConfig
	}
	c.retry.MaxFailures = uint32(failures)
	if _, e = retry.New(c.retry); e != nil || c.recoveryInterval < time.Second || c.leaseTTL > time.Minute || c.timeout > c.leaseTTL || c.telegram.Timeout > c.timeout {
		return config{}, errConfig
	}
	c.telegram.Token = get("TELEGRAM_BOT_TOKEN")
	c.telegram.BaseURL = get("TELEGRAM_BASE_URL")
	// Endpoint is an origin, so a misplaced token in a path cannot become a log
	// field or a second token in the constructed sendMessage URL.
	u, e := url.Parse(c.telegram.BaseURL)
	if e != nil || u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawPath != "" {
		return config{}, errConfig
	}
	if _, e = telegram.New(c.telegram); e != nil {
		return config{}, errConfig
	}
	parts := strings.Split(c.telegram.Token, ":")
	if len(parts) != 2 || parts[1] == "" {
		return config{}, errConfig
	}
	bot, e := strconv.ParseInt(parts[0], 10, 64)
	if e != nil || bot <= 0 || strconv.FormatInt(bot, 10) != parts[0] {
		return config{}, errConfig
	}
	c.botID = parts[0]
	return c, nil
}
