// Package thresholdindex selects subscriptions by exact current-price ranges.
// It is a rebuildable in-memory index, not a durable projection or an arbiter
// of subscription versions, lifecycle, quote readiness or delivery permission.
package thresholdindex

import (
	"errors"
	"strings"
	"sync"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
)

var ErrEntry = errors.New("thresholdindex: invalid entry")

// Entry is an immutable value copied from the caller's current projection.
// Version is returned unchanged for the subsequent canonical API check.
type Entry struct {
	ID        string
	Version   uint64
	Pair      string
	Threshold price.Price
	Direction price.Direction
}

type group struct {
	pair      string
	direction price.Direction
}

func (e Entry) group() group { return group{e.Pair, e.Direction} }

// Index has a usable zero value. Methods are safe for concurrent calls. The
// caller must serialize projection changes in authoritative order: Put replaces
// an ID unconditionally, and Remove must accompany cancellation/exit from active.
// Never copy an Index after first use.
type Index struct {
	mu      sync.RWMutex
	entries map[string]Entry
	roots   map[group]*node
}

// Put inserts or replaces an active subscription. Invalid input leaves the
// index unchanged. It does not persist, validate lifecycle or arbitrate versions.
func (x *Index) Put(e Entry) error {
	if strings.TrimSpace(e.ID) == "" || e.Version == 0 || e.Pair == "" || (e.Direction != price.Up && e.Direction != price.Down) {
		return ErrEntry
	}
	for _, r := range e.Pair {
		if !(r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return ErrEntry
		}
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.entries == nil {
		x.entries = make(map[string]Entry)
		x.roots = make(map[group]*node)
	}
	if old, ok := x.entries[e.ID]; ok {
		x.remove(old)
	}
	x.roots[e.group()] = insert(x.roots[e.group()], e)
	x.entries[e.ID] = e
	return nil
}

// Remove deletes an ID, including its threshold. Missing IDs are harmless.
func (x *Index) Remove(id string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if old, ok := x.entries[id]; ok {
		x.remove(old)
		delete(x.entries, id)
	}
}
func (x *Index) remove(e Entry) {
	key := e.group()
	root := erase(x.roots[key], e)
	if root == nil {
		delete(x.roots, key)
	} else {
		x.roots[key] = root
	}
}

// Candidates returns a detached slice: Up thresholds <= current and Down
// thresholds >= current, only for pair. Both bounds are inclusive. Order is
// unspecified; repeated calls do not consume entries or require a crossing.
// Each group uses an AVL tree, so lookup costs O(log n + k) for k results.
func (x *Index) Candidates(pair string, current price.Price) []Entry {
	x.mu.RLock()
	defer x.mu.RUnlock()
	var result []Entry
	collect(x.roots[group{pair, price.Up}], current, price.Up, &result)
	collect(x.roots[group{pair, price.Down}], current, price.Down, &result)
	return result
}

type node struct {
	entry       Entry
	left, right *node
	height      int
}

func height(n *node) int {
	if n == nil {
		return 0
	}
	return n.height
}
func refresh(n *node) { n.height = 1 + max(height(n.left), height(n.right)) }
func rotateRight(n *node) *node {
	p := n.left
	n.left = p.right
	p.right = n
	refresh(n)
	refresh(p)
	return p
}
func rotateLeft(n *node) *node {
	p := n.right
	n.right = p.left
	p.left = n
	refresh(n)
	refresh(p)
	return p
}
func balance(n *node) *node {
	if n == nil {
		return nil
	}
	refresh(n)
	if height(n.left)-height(n.right) > 1 {
		if height(n.left.left) < height(n.left.right) {
			n.left = rotateLeft(n.left)
		}
		return rotateRight(n)
	}
	if height(n.right)-height(n.left) > 1 {
		if height(n.right.right) < height(n.right.left) {
			n.right = rotateRight(n.right)
		}
		return rotateLeft(n)
	}
	return n
}

// ID breaks ties without losing subscriptions at an identical exact threshold.
func compare(a, b Entry) int {
	if c := a.Threshold.Cmp(b.Threshold); c != 0 {
		return c
	}
	return strings.Compare(a.ID, b.ID)
}
func insert(n *node, e Entry) *node {
	if n == nil {
		return &node{entry: e, height: 1}
	}
	if compare(e, n.entry) < 0 {
		n.left = insert(n.left, e)
	} else {
		n.right = insert(n.right, e)
	}
	return balance(n)
}
func erase(n *node, e Entry) *node {
	if n == nil {
		return nil
	}
	switch c := compare(e, n.entry); {
	case c < 0:
		n.left = erase(n.left, e)
	case c > 0:
		n.right = erase(n.right, e)
	default:
		if n.left == nil {
			return n.right
		}
		if n.right == nil {
			return n.left
		}
		successor := n.right
		for successor.left != nil {
			successor = successor.left
		}
		n.entry = successor.entry
		n.right = erase(n.right, successor.entry)
	}
	return balance(n)
}
func collect(n *node, current price.Price, direction price.Direction, result *[]Entry) {
	if n == nil {
		return
	}
	c := n.entry.Threshold.Cmp(current)
	if direction == price.Up && c > 0 {
		collect(n.left, current, direction, result)
		return
	}
	if direction == price.Down && c < 0 {
		collect(n.right, current, direction, result)
		return
	}
	collect(n.left, current, direction, result)
	*result = append(*result, n.entry)
	collect(n.right, current, direction, result)
}
