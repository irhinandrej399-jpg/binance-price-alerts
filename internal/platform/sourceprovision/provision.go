// Package sourceprovision implements explicit, offline source provisioning.
package sourceprovision

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
)

var ErrInvalid = errors.New("source provisioning: invalid or incomplete inventory/dataset")
var safeName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$`)
var counts = map[string]int{"prices": 16, "subscription-changes": 1, "delivery-requests": 16}

type Broker interface {
	Metadata(context.Context, string) (kafkasource.Identity, []int32, error)
	Empty(context.Context, string, []int32) error
}
type Store interface {
	Bootstrap(context.Context, intake.Binding, []int32) error
	ReadProgress(context.Context, intake.Binding, int32) (intake.Progress, error)
}

// Inventory is created once before any SQL mutation. It contains no credentials.
// A saved inventory does not prove SQL bootstrap: always run verify before start.
type Inventory struct {
	Version         int
	Environment     string
	Sources         map[string]kafkasource.Identity
	Binding         intake.Binding
	Runtime         map[string]string
	PricesBootstrap map[int32]int64
}

func partitions(n int) []int32 {
	r := make([]int32, n)
	for i := range r {
		r[i] = int32(i)
	}
	return r
}
func validParts(ps []int32, n int) bool {
	if len(ps) != n {
		return false
	}
	seen := map[int32]bool{}
	for _, p := range ps {
		if p < 0 || int(p) >= n || seen[p] {
			return false
		}
		seen[p] = true
	}
	return true
}
func (i Inventory) validate() error {
	if i.Version != 1 || !safeName.MatchString(i.Environment) || len(i.Sources) != 3 || len(i.PricesBootstrap) != 16 || len(i.Runtime) != 8 {
		return ErrInvalid
	}
	cluster := ""
	ids := map[[16]byte]bool{}
	for name := range counts {
		s := i.Sources[name]
		if s.Validate() != nil || s.Topic != i.Environment+"-"+name || ids[s.TopicID] {
			return ErrInvalid
		}
		if cluster != "" && cluster != s.ClusterID {
			return ErrInvalid
		}
		cluster = s.ClusterID
		ids[s.TopicID] = true
	}
	for p := int32(0); p < 16; p++ {
		v, ok := i.PricesBootstrap[p]
		if !ok || v != 0 {
			return ErrInvalid
		}
	}
	if i.Binding.Source != i.Sources["delivery-requests"] || i.Binding.Validate() != nil || !safeName.MatchString(i.Binding.Generation) || i.Binding.ConsumerID != i.Environment+"-notifications" {
		return ErrInvalid
	}
	expected := runtimeIDs(i.Environment)
	for k, v := range expected {
		if i.Runtime[k] != v {
			return ErrInvalid
		}
	}
	return nil
}
func runtimeIDs(prefix string) map[string]string {
	return map[string]string{
		"MATCHER_ENVIRONMENT": prefix, "MATCHER_GROUP": prefix + "-matcher", "MATCHER_REPLICA_ID": "one",
		"NOTIFICATIONS_GROUP": prefix + "-notifications", "MARKET_DATA_RELAY_TRANSACTIONAL_ID": prefix + "-market-relay",
		"SUBSCRIPTION_RELAY_TRANSACTIONAL_ID": prefix + "-api-changes", "DELIVERY_RELAY_TRANSACTIONAL_ID": prefix + "-api-delivery",
		"NOTIFICATIONS_CONSUMER_ID": prefix + "-notifications",
	}
}

// Env returns settings to merge into the corresponding service's environment.
func (i Inventory) Env() map[string]string {
	env := map[string]string{}
	for k, v := range i.Runtime {
		env[k] = v
	}
	env["KAFKA_CLUSTER_ID"] = i.Binding.Source.ClusterID
	for name, key := range map[string]string{"prices": "PRICES", "subscription-changes": "SUBSCRIPTION_CHANGES", "delivery-requests": "DELIVERY_REQUESTS"} {
		s := i.Sources[name]
		env[key+"_TOPIC"] = s.Topic
		env[key+"_TOPIC_ID"] = hex.EncodeToString(s.TopicID[:])
	}
	env["NOTIFICATIONS_GENERATION"] = i.Binding.Generation
	vector, _ := json.Marshal(i.PricesBootstrap)
	env["PRICES_BOOTSTRAP"] = string(vector)
	return env
}
func Load(path string) (Inventory, error) {
	var i Inventory
	f, err := os.Open(path)
	if err != nil {
		return i, ErrInvalid
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 65537))
	d.DisallowUnknownFields()
	if d.Decode(&i) != nil {
		return Inventory{}, ErrInvalid
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return Inventory{}, ErrInvalid
	}
	if err = i.validate(); err != nil {
		return Inventory{}, err
	}
	return i, nil
}

// save uses a synced temporary file and an exclusive hard link: never replace
// existing IDs, even on concurrent init. Sync the directory before touching SQL.
func save(path string, i Inventory) error {
	data, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return ErrInvalid
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".source-inventory-*")
	if err != nil {
		return ErrInvalid
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(append(data, '\n')); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return ErrInvalid
	}
	if os.Link(f.Name(), path) != nil {
		return ErrInvalid
	}
	d, err := os.Open(dir)
	if err != nil {
		return ErrInvalid
	}
	defer d.Close()
	if d.Sync() != nil {
		return ErrInvalid
	}
	return nil
}
func checkSources(ctx context.Context, i Inventory, b Broker, empty bool) error {
	for name, n := range counts {
		expected := i.Sources[name]
		actual, ps, err := b.Metadata(ctx, expected.Topic)
		if err != nil {
			return err
		}
		if actual != expected || !validParts(ps, n) {
			return ErrInvalid
		}
		if empty {
			if err = b.Empty(ctx, expected.Topic, ps); err != nil {
				return err
			}
		}
	}
	return nil
}
func progress(ctx context.Context, i Inventory, s Store) (bool, error) {
	missing := 0
	for _, p := range partitions(16) {
		v, err := s.ReadProgress(ctx, i.Binding, p)
		if errors.Is(err, intake.ErrMissing) {
			missing++
			continue
		}
		if err != nil {
			return false, err
		}
		if v.Binding != i.Binding || v.Partition != p || v.Next < 0 {
			return false, ErrInvalid
		}
	}
	if missing != 0 && missing != 16 {
		return false, ErrInvalid
	}
	return missing == 16, nil
}

// Run requires all producers/consumers stopped. init refuses an existing path;
// resume may bootstrap only if every SQL partition is absent and Kafka is empty.
// verify is read-only and does not require empty history for an existing binding.
func Run(ctx context.Context, mode, path, prefix string, b Broker, s Store) error {
	if path == "" || b == nil || s == nil {
		return ErrInvalid
	}
	var i Inventory
	if mode == "init" {
		if !safeName.MatchString(prefix) {
			return ErrInvalid
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			return ErrInvalid
		}
		i = Inventory{Version: 1, Environment: prefix, Sources: map[string]kafkasource.Identity{}, Runtime: runtimeIDs(prefix), PricesBootstrap: map[int32]int64{}}
		for name, n := range counts {
			source, ps, err := b.Metadata(ctx, prefix+"-"+name)
			if err != nil {
				return err
			}
			if !validParts(ps, n) {
				return ErrInvalid
			}
			i.Sources[name] = source
		}
		generation := make([]byte, 16)
		if _, err := rand.Read(generation); err != nil {
			return ErrInvalid
		}
		i.Binding = intake.Binding{ConsumerID: prefix + "-notifications", Generation: hex.EncodeToString(generation), Source: i.Sources["delivery-requests"]}
		for p := int32(0); p < 16; p++ {
			i.PricesBootstrap[p] = 0
		}
		if err := i.validate(); err != nil {
			return err
		}
		if err := checkSources(ctx, i, b, true); err != nil {
			return err
		}
		if err := save(path, i); err != nil {
			return err
		}
	} else if mode == "resume" || mode == "verify" {
		var err error
		i, err = Load(path)
		if err != nil {
			return err
		}
		if prefix != "" && prefix != i.Environment {
			return ErrInvalid
		}
	} else {
		return ErrInvalid
	}
	if err := checkSources(ctx, i, b, false); err != nil {
		return err
	}
	missing, err := progress(ctx, i, s)
	if err != nil {
		return err
	}
	if missing {
		if mode == "verify" {
			return intake.ErrMissing
		}
		if err = checkSources(ctx, i, b, true); err != nil {
			return err
		}
		if err = s.Bootstrap(ctx, i.Binding, partitions(16)); err != nil {
			return err
		}
	}
	missing, err = progress(ctx, i, s)
	if err != nil {
		return err
	}
	if missing {
		return intake.ErrMissing
	}
	return checkSources(ctx, i, b, false)
}
