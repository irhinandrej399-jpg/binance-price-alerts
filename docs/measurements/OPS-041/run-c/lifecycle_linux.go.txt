package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"syscall"
)

func lifecycleEnabled() bool { return os.Getenv("OPS039_LIFECYCLE") == "1" }
func (c *capture) stopWitness(pid int, status syscall.WaitStatus) {
	pending := map[int]any{}
	for tid, t := range c.tasks {
		pending[tid] = map[string]any{"nr": t.nr, "args": t.args, "entered": t.entered, "newborn": t.newborn}
	}
	c.emit("wait_stop", map[string]any{"pid": pid, "status": int(status), "active": c.active, "queue": append([]int{}, c.queue...), "pending": pending, "identity": readEvidence(fmt.Sprintf("/proc/%d/status", pid)), "stat": readEvidence(fmt.Sprintf("/proc/%d/stat", pid))})
}

func (c *capture) dropQueue(ids map[int]bool) {
	q := []int{}
	for _, p := range c.queue {
		if !ids[p] {
			q = append(q, p)
		}
	}
	c.queue = q
}
func (c *capture) execTransition(pid, former int) *task {
	source := c.tasks[former]
	if source == nil {
		c.issue("exec former TID not registered")
		source = &task{tgid: pid, newborn: true}
	}
	retired := []int{}
	abandoned := map[int]any{}
	ids := map[int]bool{}
	for tid, t := range c.tasks {
		if t.tgid == source.tgid && tid != former {
			retired = append(retired, tid)
			ids[tid] = true
			abandoned[tid] = map[string]any{"nr": t.nr, "args": t.args, "entered": t.entered, "newborn": t.newborn}
			delete(c.tasks, tid)
		}
	}
	sort.Ints(retired)
	activeBefore := c.active
	queueBefore := append([]int{}, c.queue...)
	delete(c.tasks, former)
	ids[former] = true
	c.dropQueue(ids)
	if c.active == former {
		c.active = pid
	} else if ids[c.active] {
		c.active = 0
		c.issue("exec abandoned active sibling")
	}
	c.tasks[pid] = source
	source.tgid = pid
	c.emit("exec_transition", map[string]any{"pid": pid, "former": former, "retired": retired, "abandoned": abandoned, "pending_nr": source.nr, "pending_args": source.args, "pending_entered": source.entered, "active_before": activeBefore, "active_after": c.active, "queue_before": queueBefore, "queue_after": append([]int{}, c.queue...), "identity": readEvidence(fmt.Sprintf("/proc/%d/status", pid)), "stat": readEvidence(fmt.Sprintf("/proc/%d/stat", pid))})
	return source
}
func pathMemory(pid int, address uint64) memory {
	m := readMemory(pid, address, 4096)
	data, _ := hex.DecodeString(m.Hex)
	for i, b := range data {
		if b == 0 {
			m.Hex = hex.EncodeToString(data[:i+1])
			m.Length = uint64(i + 1)
			m.Error = ""
			return m
		}
	}
	if m.Error == "" {
		m.Error = "path exceeds 4096 bytes without NUL"
	}
	return m
}
func (c *capture) cloneFlags(pid int, t *task) uint64 {
	if t.flagsKnown {
		return t.flags
	}
	c.issue("clone flags unavailable")
	return 0
}
func (c *capture) lifecycleInput(pid int, t *task) map[string]any {
	t.flagsKnown = false
	t.cloneData = nil
	data := map[string]any{"version": "linux-arm64-lifecycle-v1", "paths": []any{}, "clone3": nil, "flags": nil, "incomplete": false}
	indices := []int{}
	switch t.nr {
	case 56, 437, 221, 281:
		indices = []int{1}
		if t.nr == 221 {
			indices = []int{0}
		}
	case 35, 78, 79, 48:
		indices = []int{1}
	case 36:
		indices = []int{0, 2}
	case 37, 38, 276:
		indices = []int{1, 3}
	}
	paths := []any{}
	for _, i := range indices {
		m := pathMemory(pid, t.args[i])
		paths = append(paths, map[string]any{"arg": i, "memory": m})
		if m.Error != "" {
			data["incomplete"] = true
			c.issue("lifecycle path unreadable")
		}
	}
	data["paths"] = paths
	if t.nr == 435 {
		m := readMemory(pid, t.args[0], t.args[1])
		data["clone3"] = m
		if m.Error != "" || (t.args[1] != 64 && t.args[1] != 80 && t.args[1] != 88) {
			data["incomplete"] = true
			c.issue("clone3 unreadable or unsupported size")
		} else {
			b, _ := hex.DecodeString(m.Hex)
			t.cloneData = append([]byte{}, b...)
			data["flags"] = binary.LittleEndian.Uint64(b[:8])
			t.flags = binary.LittleEndian.Uint64(b[:8])
			t.flagsKnown = true
		}
	}
	if t.nr == 220 {
		data["flags"] = t.args[0]
		t.flags = t.args[0]
		t.flagsKnown = true
	}
	if t.nr == 437 {
		data["open_how"] = readMemory(pid, t.args[2], t.args[3])
		if t.args[3] != 24 || data["open_how"].(memory).Error != "" {
			data["incomplete"] = true
			c.issue("open_how unreadable or unsupported size")
		}
	}
	if t.flagsKnown && t.flags&(0x100|0x400) != 0 {
		c.issue("shared FD/VM observations lack an all-owner barrier")
	}
	return data
}

// Parent-side pointed-to IDs are observed on return, using pointers saved at
// entry, never a subsequently mutated clone_args structure. Shared reads are
// still non-atomic; child-side TID/clear_tid lifetime semantics remain unresolved.
func (c *capture) lifecycleOutput(pid int, t *task, ret int64) []any {
	out := []any{}
	if ret < 0 || !t.flagsKnown {
		return out
	}
	add := func(name string, ptr uint64) {
		m := readMemory(pid, ptr, 4)
		out = append(out, map[string]any{"name": name, "memory": m})
		if m.Error != "" {
			c.issue("clone output identity unreadable")
		}
	}
	if t.nr == 220 {
		if t.flags&0x1000 != 0 {
			add("pidfd", t.args[2])
		}
		if t.flags&0x100000 != 0 {
			add("parent_tid", t.args[2])
		}
	}
	if t.nr == 435 && len(t.cloneData) >= 64 {
		if t.flags&0x1000 != 0 {
			add("pidfd", binary.LittleEndian.Uint64(t.cloneData[8:16]))
		}
		if t.flags&0x100000 != 0 {
			add("parent_tid", binary.LittleEndian.Uint64(t.cloneData[24:32]))
		}
	}
	if t.flags&(0x1000000|0x200000) != 0 {
		c.issue("child TID/clear_tid pointed-to lifecycle unresolved")
	}
	return out
}
