// Package cleaner implements the local maintenance registry and safe Kafka
// prefix cleanup. All participants must share one authoritative local directory.
package cleaner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
)

var (
	ErrState  = errors.New("cleaner registry missing, invalid or unavailable")
	ErrConfig = errors.New("invalid cleaner configuration")
	ErrProof  = errors.New("required consumer progress is not confirmed")
	ErrBroker = errors.New("Kafka source or retained bounds unavailable")
	ErrDelete = errors.New("DeleteRecords outcome is not confirmed")
)

const maxConsumers = 1024

// Consumer names an exact SQL consumer. Endpoint is an alias in caller config,
// never a DSN, credential or a transient group member. Retired is persisted only.
type Consumer struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"` // projection, prices, notifications
	ID         string `json:"id"`
	Generation string `json:"generation,omitempty"`
	Endpoint   string `json:"endpoint"`
	Retired    bool   `json:"retired,omitempty"`
}

func bounded(s string) bool { return strings.TrimSpace(s) != "" && len(s) <= 256 }
func (c Consumer) valid() bool {
	return bounded(c.Name) && bounded(c.ID) && bounded(c.Endpoint) && ((c.Kind == "notifications" && bounded(c.Generation)) || ((c.Kind == "projection" || c.Kind == "prices") && c.Generation == ""))
}
func (c Consumer) identity() string { return c.Kind + "\x00" + c.ID + "\x00" + c.Generation }

type Inventory struct {
	Source    kafkasource.Identity `json:"source"`
	Partition int32                `json:"partition"`
	Consumers []Consumer           `json:"consumers"`
}
type State struct {
	Version int `json:"version"`
	Inventory
	// Floor is a durable upper bound on all DeleteRecords requests ever issued,
	// including requests whose response was lost. It never decreases.
	Floor int64 `json:"floor"`
}

func (s State) valid() bool {
	if s.Version != 1 || s.Source.Validate() != nil || s.Partition < 0 || s.Floor < 0 || len(s.Consumers) == 0 || len(s.Consumers) > maxConsumers {
		return false
	}
	names, ids := map[string]bool{}, map[string]bool{}
	for _, c := range s.Consumers {
		if !c.valid() || names[c.Name] || ids[c.identity()] {
			return false
		}
		names[c.Name] = true
		ids[c.identity()] = true
	}
	return true
}

type Store struct{ dir string }

// NewStore never creates or initializes registry state. Provision a private
// local directory first; loss/rollback of that directory is an operator fault.
func NewStore(dir string) *Store {
	if dir == "" {
		return &Store{}
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return &Store{}
	}
	return &Store{dir: absolute}
}
func (s *Store) path() string { return filepath.Join(s.dir, "registry.json") }
func (s *Store) locked(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stat, e := os.Lstat(s.dir)
	if e != nil || !stat.IsDir() || stat.Mode().Perm()&0022 != 0 {
		return ErrState
	}
	fd, e := syscall.Open(filepath.Join(s.dir, "registry.lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return ErrState
	}
	f := os.NewFile(uintptr(fd), "registry lock")
	defer f.Close()
	stat, e = f.Stat()
	if e != nil || !stat.Mode().IsRegular() {
		return ErrState
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		e = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if e == nil {
			break
		}
		if e != syscall.EWOULDBLOCK && e != syscall.EAGAIN {
			return ErrState
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer syscall.Flock(fd, syscall.LOCK_UN)
	return fn(ctx)
}
func (s *Store) load() (State, error) {
	var state State
	fd, e := syscall.Open(s.path(), syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if e != nil {
		return state, ErrState
	}
	f := os.NewFile(uintptr(fd), "registry state")
	defer f.Close()
	stat, e := f.Stat()
	if e != nil || !stat.Mode().IsRegular() || stat.Size() > 1<<20 {
		return state, ErrState
	}
	data, e := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if e != nil || len(data) > 1<<20 {
		return state, ErrState
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if dec.Decode(&state) != nil || dec.Decode(new(any)) != io.EOF || !state.valid() {
		return State{}, ErrState
	}
	return state, nil
}
func (s *Store) save(state State) error {
	if !state.valid() {
		return ErrState
	}
	data, e := json.Marshal(state)
	if e != nil || len(data) > 1<<20 {
		return ErrState
	}
	f, e := os.CreateTemp(s.dir, ".registry-*")
	if e != nil {
		return ErrState
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	if _, e = f.Write(data); e != nil {
		return ErrState
	}
	if e = f.Sync(); e != nil {
		return ErrState
	}
	if e = f.Close(); e != nil {
		return ErrState
	}
	if e = os.Rename(name, s.path()); e != nil {
		return ErrState
	}
	d, e := os.Open(s.dir)
	if e != nil {
		return ErrState
	}
	defer d.Close()
	if d.Sync() != nil {
		return ErrState
	}
	return nil
}
func (s *Store) Inspect(ctx context.Context) (State, error) {
	var state State
	e := s.locked(ctx, func(context.Context) error { var e error; state, e = s.load(); return e })
	return state, e
}
