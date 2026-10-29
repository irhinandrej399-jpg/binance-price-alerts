package thresholdindex

import (
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
)

func decimal(s string) price.Price {
	p, err := price.Parse(s)
	if err != nil {
		panic(err)
	}
	return p
}
func entry(id, pair, threshold string, direction price.Direction) Entry {
	return Entry{ID: id, Version: 1, Pair: pair, Threshold: decimal(threshold), Direction: direction}
}
func put(t *testing.T, x *Index, e Entry) {
	t.Helper()
	if err := x.Put(e); err != nil {
		t.Fatal(err)
	}
}
func ids(entries []Entry) []string {
	result := make([]string, 0, len(entries))
	for _, e := range entries {
		result = append(result, e.ID)
	}
	slices.Sort(result)
	return result
}
func wantIDs(t *testing.T, x *Index, pair, p string, want ...string) {
	t.Helper()
	slices.Sort(want)
	if got := ids(x.Candidates(pair, decimal(p))); !slices.Equal(got, want) {
		t.Fatalf("%s @ %s: got %v want %v", pair, p, got, want)
	}
}

func TestExactInclusiveRanges(t *testing.T) {
	var x Index
	for _, e := range []Entry{
		entry("u-low", "BTCUSDT", "99.999999999999999999", price.Up),
		entry("u-eq", "BTCUSDT", "100.00", price.Up), entry("u-dup", "BTCUSDT", "100", price.Up),
		entry("u-high", "BTCUSDT", "100.000000000000000001", price.Up),
		entry("d-low", "BTCUSDT", "99.999999999999999999", price.Down),
		entry("d-eq", "BTCUSDT", "100", price.Down), entry("d-high", "BTCUSDT", "100.000000000000000001", price.Down),
		entry("other", "ETHUSDT", "100", price.Up),
	} {
		put(t, &x, e)
	}
	wantIDs(t, &x, "BTCUSDT", "100", "u-low", "u-eq", "u-dup", "d-eq", "d-high")
	wantIDs(t, &x, "BTCUSDT", "0", "d-low", "d-eq", "d-high")
	wantIDs(t, &x, "BTCUSDT", "999999999999999999.999999999999999999", "u-low", "u-eq", "u-dup", "u-high")
	wantIDs(t, &x, "ETHUSDT", "100", "other")
	wantIDs(t, &x, "UNKNOWN", "100")
	// Repeated true prices do not require a new crossing and do not consume entries.
	wantIDs(t, &x, "ETHUSDT", "101", "other")
	wantIDs(t, &x, "ETHUSDT", "99")
	wantIDs(t, &x, "ETHUSDT", "101", "other")
}

func TestReplaceRemoveAndDetachedResults(t *testing.T) {
	var x Index
	e := entry("a", "BTCUSDT", "100", price.Up)
	put(t, &x, e)
	put(t, &x, entry("b", "BTCUSDT", "100", price.Up))
	put(t, &x, e) // duplicate ID does not add a duplicate candidate
	wantIDs(t, &x, "BTCUSDT", "100", "a", "b")
	got := x.Candidates("BTCUSDT", decimal("100"))
	got[0].ID = "mutated"
	_ = got[0].Threshold.UnmarshalText([]byte("999"))
	wantIDs(t, &x, "BTCUSDT", "100", "a", "b")
	e.Pair = "ETHUSDT"
	e.Direction = price.Down
	e.Threshold = decimal("200")
	e.Version = 3
	put(t, &x, e)
	wantIDs(t, &x, "BTCUSDT", "100", "b")
	wantIDs(t, &x, "ETHUSDT", "200", "a")
	if result := x.Candidates("ETHUSDT", decimal("200")); len(result) != 1 || result[0].Version != 3 {
		t.Fatal(result)
	}
	x.Remove("a")
	x.Remove("a")
	x.Remove("missing")
	x.Remove("b")
	wantIDs(t, &x, "BTCUSDT", "100")
	wantIDs(t, &x, "ETHUSDT", "200")
}

func TestInvalidPutDoesNotReplace(t *testing.T) {
	var x Index
	e := entry("a", "BTCUSDT", "100", price.Up)
	put(t, &x, e)
	for _, mutate := range []func(*Entry){func(e *Entry) { e.ID = " " }, func(e *Entry) { e.Version = 0 }, func(e *Entry) { e.Pair = "" }, func(e *Entry) { e.Pair = "btc-usdt" }, func(e *Entry) { e.Direction = "sideways" }} {
		bad := e
		mutate(&bad)
		if err := x.Put(bad); err == nil {
			t.Fatalf("accepted %+v", bad)
		}
		wantIDs(t, &x, "BTCUSDT", "100", "a")
	}
}

func TestRandomUpdatesAgainstPredicate(t *testing.T) {
	var x Index
	model := map[string]Entry{}
	rng := rand.New(rand.NewSource(42))
	for step := 0; step < 3000; step++ {
		id := fmt.Sprint(rng.Intn(200))
		if rng.Intn(4) == 0 {
			x.Remove(id)
			delete(model, id)
		} else {
			direction := price.Up
			if rng.Intn(2) == 0 {
				direction = price.Down
			}
			e := entry(id, fmt.Sprintf("PAIR%d", rng.Intn(4)), fmt.Sprintf("%d.%018d", rng.Intn(20), rng.Intn(5)), direction)
			put(t, &x, e)
			model[id] = e
		}
		pair := fmt.Sprintf("PAIR%d", rng.Intn(4))
		current := decimal(fmt.Sprintf("%d.%018d", rng.Intn(20), rng.Intn(5)))
		var want []Entry
		for _, e := range model {
			if e.Pair == pair && price.Satisfied(current, e.Threshold, e.Direction) {
				want = append(want, e)
			}
		}
		if !reflect.DeepEqual(ids(x.Candidates(pair, current)), ids(want)) {
			t.Fatalf("step %d", step)
		}
	}
}

func BenchmarkCandidates(b *testing.B) {
	for _, hot := range []bool{false, true} {
		for _, dense := range []bool{false, true} {
			b.Run(fmt.Sprintf("hot=%v/dense=%v", hot, dense), func(b *testing.B) {
				var x Index
				for i := 0; i < 100000; i++ {
					pair := fmt.Sprintf("PAIR%d", i%100)
					if hot && i%2 == 0 {
						pair = "PAIR0"
					}
					e := entry(fmt.Sprint(i), pair, fmt.Sprint(i+1), price.Up)
					if err := x.Put(e); err != nil {
						b.Fatal(err)
					}
				}
				current := decimal("100")
				if dense {
					current = decimal("100000")
				}
				expected := 1
				if hot {
					expected = 50
				}
				if dense {
					expected = 1000
					if hot {
						expected = 50000
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if got := len(x.Candidates("PAIR0", current)); got != expected {
						b.Fatal(got)
					}
				}
				b.ReportMetric(float64(expected), "candidates/op")
			})
		}
	}
}

// Ordered insertion and deletion must keep logarithmic depth, including a hot
// group where every subscription has the same threshold.
func TestBalanceUnderOrderedAndEqualKeys(t *testing.T) {
	for _, equal := range []bool{false, true} {
		var x Index
		for i := 0; i < 10000; i++ {
			threshold := fmt.Sprint(i)
			if equal {
				threshold = "100"
			}
			put(t, &x, entry(fmt.Sprintf("%05d", i), "BTCUSDT", threshold, price.Up))
		}
		var check func(*node) int
		check = func(n *node) int {
			if n == nil {
				return 0
			}
			left, right := check(n.left), check(n.right)
			if left-right > 1 || right-left > 1 || n.height != 1+max(left, right) {
				t.Fatalf("unbalanced tree at %s", n.entry.ID)
			}
			return 1 + max(left, right)
		}
		if h := check(x.roots[group{"BTCUSDT", price.Up}]); h > 20 {
			t.Fatalf("depth %d", h)
		}
		for i := 0; i < 10000; i++ {
			x.Remove(fmt.Sprintf("%05d", i))
			if i%100 == 0 {
				check(x.roots[group{"BTCUSDT", price.Up}])
			}
		}
		if len(x.entries) != 0 || len(x.roots) != 0 {
			t.Fatal("empty group retained")
		}
	}
}

func TestConcurrentReadersAndWriters(t *testing.T) {
	var x Index
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			for i := 0; i < 200; i++ {
				id := fmt.Sprintf("%d-%d", worker, i)
				if err := x.Put(entry(id, "BTCUSDT", "100", price.Up)); err != nil {
					t.Error(err)
				}
				_ = x.Candidates("BTCUSDT", decimal("100"))
				x.Remove(id)
			}
		})
	}
	wg.Wait()
	wantIDs(t, &x, "BTCUSDT", "100")
}
