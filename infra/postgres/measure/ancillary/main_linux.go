// Ancillary capture for OPS-033, derived from the OPS-031 research gate. This is NOT the qualified temp peak observer.
// Run only in a disposable PID/filesystem namespace. Linux arm64 only.
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

type task struct {
	cloneData        []byte
	flags            uint64
	flagsKnown       bool
	tgid             int
	nr               uint64
	args             [6]uint64
	entered, newborn bool
}
type file struct {
	Dev   uint64 `json:"dev"`
	Ino   uint64 `json:"ino"`
	Size  int64  `json:"size"`
	Nlink uint64 `json:"nlink"`
	Mode  uint32 `json:"mode"`
	Path  string `json:"path"`
	PID   int    `json:"pid,omitempty"`
	FD    int    `json:"fd,omitempty"`
}
type capture struct {
	tasks  map[int]*task
	active int
	queue  []int
	enc    *json.Encoder
	seq    int
	flush  func()
	t0     time.Time
	root   string
	issues map[string]bool
}

func (c *capture) emit(kind string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	c.seq++
	data["kind"], data["seq"], data["ns"] = kind, c.seq, time.Since(c.t0).Nanoseconds()
	if err := c.enc.Encode(data); err != nil {
		panic(err)
	}
	if c.flush != nil {
		c.flush()
	}
}
func (c *capture) issue(s string) {
	if !c.issues[s] {
		c.issues[s] = true
		c.emit("unsupported", map[string]any{"reason": s})
	}
}
func stat(path string) (file, error) {
	var s syscall.Stat_t
	err := syscall.Stat(path, &s)
	return file{Dev: s.Dev, Ino: s.Ino, Size: s.Size, Nlink: uint64(s.Nlink), Mode: s.Mode, Path: path}, err
}
func regular(path string) bool {
	var s syscall.Stat_t
	return syscall.Stat(path, &s) == nil && s.Mode&syscall.S_IFMT == syscall.S_IFREG
}
func (c *capture) work(path string) bool {
	path = strings.TrimSuffix(path, " (deleted)")
	return path == c.root || strings.HasPrefix(path, c.root+"/")
}
func (c *capture) snapshot(phase string, pid int) {
	if inventoryEnabled() {
		c.inventory(phase, pid)
		return
	}
	// All operations classified as mutations are stopped/queued, except the
	// one just completed. This assumption is explicitly NOT yet proven complete.
	var files []file
	var tids []int
	for tid := range c.tasks {
		tids = append(tids, tid)
	}
	sort.Ints(tids)
	for _, tid := range tids {
		dir := fmt.Sprintf("/proc/%d/fd", tid)
		entries, err := os.ReadDir(dir)
		if err != nil {
			c.issue("fd inventory: " + err.Error())
			continue
		}
		for _, entry := range entries {
			p := dir + "/" + entry.Name()
			link, err := os.Readlink(p)
			if err != nil {
				c.issue("readlink: " + err.Error())
				continue
			}
			if !c.work(link) {
				continue
			}
			if !regular(p) {
				continue
			}
			f, err := stat(p)
			if err != nil {
				c.issue("stat: " + err.Error())
				continue
			}
			f.Path, f.PID = link, tid
			f.FD, _ = strconv.Atoi(entry.Name())
			files = append(files, f)
		}
	}
	err := filepath.WalkDir(c.root, func(path string, d os.DirEntry, err error) error {
		if os.IsNotExist(err) && path == c.root {
			return nil
		}
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !regular(path) {
			return fmt.Errorf("non-regular work entry: %s", path)
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in work root: %s", path)
		}
		f, err := stat(path)
		if err != nil {
			return err
		}
		files = append(files, f)
		return nil
	})
	if err != nil {
		c.issue("walk: " + err.Error())
	}
	c.emit("snapshot", map[string]any{"phase": phase, "pid": pid, "tids": tids, "files": files})
}
func fdRegular(pid int, fd uint64) bool {
	var s syscall.Stat_t
	return syscall.Stat(fmt.Sprintf("/proc/%d/fd/%d", pid, fd), &s) == nil && s.Mode&syscall.S_IFMT == syscall.S_IFREG
}
func (c *capture) classify(pid int, t *task) bool {
	n, a := t.nr, t.args
	if inventoryEnabled() && n == 155 {
		return true
	} // getpgid: helper checkpoint barrier on caller only.
	switch n {
	// Potentially blocking pipe/socket IO must not hold the filesystem gate.
	case 64, 66, 68, 70, 287:
		return fdRegular(pid, a[0])
	case 71, 76, 285: // sendfile, splice, copy_file_range: output fd
		fd := a[0]
		if n != 71 {
			fd = a[2]
		}
		return fdRegular(pid, fd)
	case 23, 24, 33, 34, 35, 36, 37, 38, 45, 46, 47, 49, 50, 56, 57, 93, 94, 221, 276, 281, 436, 437:
		return true
	case 220, 435:
		// In particular CLONE_VFORK waits for the child's exec/exit. Holding
		// the gate across it deadlocks the child at its first gated syscall.
		// Fork duplicates references; it does not change apparent bytes.
		return false
	case 25:
		return a[1] == 0 || a[1] == 1030 // F_DUPFD, F_DUPFD_CLOEXEC
	case 222: // A file mapping retains the inode after last close.
		link, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", pid, a[4]))
		if a[3]&syscall.MAP_ANON == 0 && c.work(link) {
			c.issue("work inode mmap")
		}
	case 0, 1, 2, 3, 4, 425, 426, 427:
		c.issue(fmt.Sprintf("asynchronous IO syscall %d", n))
	case 29:
		link, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", pid, a[0]))
		if c.work(link) {
			c.issue("work inode ioctl")
		}
	case 211, 212, 243, 269:
		// Message evidence is captured at entry/return; socket queues remain unresolved.
		c.issue("socket queue reference history pending OPS-034")
	case 39, 40, 41, 51, 97, 117, 265, 268, 270, 271, 280, 428, 429, 430, 431, 432, 433, 442:
		c.issue(fmt.Sprintf("namespace/external mutation syscall %d", n))
	}
	// This prototype intentionally records an unresolved classifier gate.
	// The verifier cannot certify unclassified calls as non-mutating.
	return false
}

func (c *capture) resume(pid, sig int) {
	if err := syscall.PtraceSyscall(pid, sig); err != nil {
		panic(err)
	}
}
func (c *capture) dispatch(pid int) {
	if c.active != 0 {
		c.queue = append(c.queue, pid)
		c.emit("queued", map[string]any{"pid": pid})
		return
	}
	c.active = pid
	c.snapshot("before", pid)
	c.emit("dispatch", map[string]any{"pid": pid})
	c.resume(pid, 0)
}
func (c *capture) release(pid int) {
	if c.active != pid {
		return
	}
	c.snapshot("after", pid)
	c.emit("release", map[string]any{"pid": pid})
	c.active = 0
	if len(c.queue) > 0 {
		p := c.queue[0]
		c.queue = c.queue[1:]
		c.dispatch(p)
	}
}
func main() {
	if len(os.Args) < 5 {
		panic("usage: capture ROOT RAW.gz -- COMMAND [ARG...]")
	}
	if runtime.GOARCH != "arm64" {
		panic("research syscall classifier is arm64 only")
	}
	runtime.LockOSThread()
	f, err := os.OpenFile(os.Args[2], os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	z := gzip.NewWriter(f)
	b := bufio.NewWriterSize(z, 1<<20)
	defer f.Close()
	defer z.Close()
	defer b.Flush()
	c := &capture{tasks: map[int]*task{}, enc: json.NewEncoder(b), t0: time.Now(), root: os.Args[1], issues: map[string]bool{}}
	if lifecycleEnabled() {
		c.flush = func() {
			if e := b.Flush(); e != nil {
				panic(e)
			}
			if e := z.Flush(); e != nil {
				panic(e)
			}
		}
	}
	schema := "ops033-ptrace-ancillary-v1"
	if inventoryEnabled() {
		schema = "ops038-ptrace-inventory-v1"
	}
	if lifecycleEnabled() {
		schema = "ops039-ptrace-lifecycle-v1"
	}
	c.emit("header", map[string]any{"schema": schema, "arch": runtime.GOARCH, "root": c.root, "command": os.Args[4:], "go_version": runtime.Version(), "ancillary_contract": "linux-arm64-ancillary-v1", "msghdr_bytes": 56, "mmsghdr_bytes": 64, "cmsghdr_bytes": 16, "control_limit": 1 << 20, "vector_limit": 1024})
	if inventoryEnabled() {
		c.inventoryEnvironment()
	}
	// Never promote a candidate maximum while the classifier proof is open.
	c.issue("complete syscall classification and monotonic-per-operation proof pending")
	cmd := exec.Command(os.Args[4], os.Args[5:]...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Ptrace: true, Pdeathsig: syscall.SIGKILL}
	if err = cmd.Start(); err != nil {
		panic(err)
	}
	rootPID := cmd.Process.Pid
	var status syscall.WaitStatus
	if _, err = syscall.Wait4(rootPID, &status, 0, nil); err != nil {
		panic(err)
	}
	// EXITKILL avoids leaving the disposable database running unobserved.
	opts := syscall.PTRACE_O_TRACESYSGOOD | syscall.PTRACE_O_TRACEFORK | syscall.PTRACE_O_TRACEVFORK | syscall.PTRACE_O_TRACECLONE | syscall.PTRACE_O_TRACEEXEC | syscall.PTRACE_O_TRACEEXIT | 0x100000
	if err = syscall.PtraceSetOptions(rootPID, opts); err != nil {
		panic(err)
	}
	c.tasks[rootPID] = &task{newborn: true, tgid: rootPID}
	c.emit("root", map[string]any{"pid": rootPID, "options": opts})
	c.snapshot("initial", rootPID)
	if err = b.Flush(); err != nil {
		panic(err)
	}
	if err = z.Flush(); err != nil {
		panic(err)
	}
	c.resume(rootPID, 0)
	rootExit := 0
	earlyStops := map[int]bool{}
	for len(c.tasks) > 0 {
		pid, err := syscall.Wait4(-1, &status, syscall.WALL, nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			panic(err)
		}
		if lifecycleEnabled() {
			c.stopWitness(pid, status)
		}
		t := c.tasks[pid]
		if t == nil {
			// waitpid may deliver a child's initial stop before the parent's
			// PTRACE_EVENT_FORK message. Keep it stopped until lineage arrives.
			if status.Stopped() && status.StopSignal() == syscall.SIGSTOP {
				earlyStops[pid] = true
				c.emit("early_child_stop", map[string]any{"pid": pid})
				continue
			}
			panic(fmt.Sprintf("unregistered tracee %d status %d", pid, status))
		}
		if status.Exited() || status.Signaled() {
			c.emit("exit", map[string]any{"pid": pid, "status": int(status), "code": status.ExitStatus(), "signal": int(status.Signal())})
			if status.Signaled() {
				c.issue("tracee killed by signal")
			}
			if pid == rootPID {
				rootExit = status.ExitStatus()
			}
			delete(c.tasks, pid)
			if lifecycleEnabled() {
				c.dropQueue(map[int]bool{pid: true})
			}
			c.release(pid)
			continue
		}
		if !status.Stopped() {
			panic("unexpected wait status")
		}
		event := int(status) >> 16
		if event != 0 {
			msg, err := syscall.PtraceGetEventMsg(pid)
			if err != nil {
				panic(err)
			}
			if lifecycleEnabled() && os.Getenv("OPS039_EARLY_CHILD") == "1" && (event == 1 || event == 2 || event == 3) && !earlyStops[int(msg)] {
				var childStatus syscall.WaitStatus
				got, e := syscall.Wait4(int(msg), &childStatus, syscall.WALL, nil)
				if e != nil {
					panic(e)
				}
				c.stopWitness(got, childStatus)
				if !childStatus.Stopped() || childStatus.StopSignal() != syscall.SIGSTOP {
					panic("expected early child SIGSTOP")
				}
				earlyStops[got] = true
				c.emit("early_child_stop", map[string]any{"pid": got})
			}
			c.emit("event", map[string]any{"pid": pid, "event": event, "message": msg})
			if event == 1 || event == 2 || event == 3 {
				if _, ok := c.tasks[int(msg)]; ok {
					panic("duplicate child")
				}
				group := int(msg)
				if lifecycleEnabled() && c.cloneFlags(pid, t)&0x10000 != 0 {
					group = t.tgid
				}
				c.tasks[int(msg)] = &task{newborn: true, tgid: group}
				if earlyStops[int(msg)] {
					delete(earlyStops, int(msg))
					c.emit("child_stop", map[string]any{"pid": int(msg)})
					c.resume(int(msg), 0)
				}
			}
			if lifecycleEnabled() && event == 4 {
				t = c.execTransition(pid, int(msg))
			}
			if lifecycleEnabled() && event != 1 && event != 2 && event != 3 && event != 4 && event != 6 {
				c.issue("unhandled ptrace event")
			}
			if event == 6 && c.active != pid {
				c.issue("exit outside serialized syscall")
			}
			c.resume(pid, 0)
			continue
		}
		if status.StopSignal() == syscall.Signal(int(syscall.SIGTRAP)|0x80) {
			var info [88]byte
			n, _, errno := syscall.Syscall6(syscall.SYS_PTRACE, 0x420e, uintptr(pid), uintptr(len(info)), uintptr(unsafe.Pointer(&info[0])), 0, 0)
			if lifecycleEnabled() {
				c.emit("syscall_info", map[string]any{"pid": pid, "bytes": n, "errno": int(errno), "hex": hex.EncodeToString(info[:])})
			}
			if errno != 0 || n < 24 || (info[0] == 1 && n < 80) || (info[0] == 2 && n < 33) || binary.LittleEndian.Uint32(info[4:8]) != 0xc00000b7 {
				if lifecycleEnabled() {
					c.issue("syscall info unreadable or unknown ABI")
					c.resume(pid, 0)
					continue
				}
				panic(fmt.Sprintf("get syscall info/ABI: %v bytes=%d", errno, n))
			}
			op := info[0]
			if op == 1 {
				if t.entered {
					panic("double syscall entry")
				}
				t.nr = binary.LittleEndian.Uint64(info[24:32])
				for i := range t.args {
					t.args[i] = binary.LittleEndian.Uint64(info[32+i*8 : 40+i*8])
				}
				t.entered = true
				t.newborn = false
				serial := c.classify(pid, t)
				row := map[string]any{"pid": pid, "nr": t.nr, "args": t.args, "serial": serial}
				if lifecycleEnabled() {
					row["lifecycle"] = c.lifecycleInput(pid, t)
				}
				if messageCall(t.nr) {
					row["messages"] = c.messages(pid, t, false, 0)
				}
				c.emit("enter", row)
				if serial {
					c.dispatch(pid)
				} else {
					c.resume(pid, 0)
				}
			} else if op == 2 {
				ret := int64(binary.LittleEndian.Uint64(info[24:32]))
				if !t.entered {
					if !t.newborn {
						panic("unmatched syscall exit")
					}
					c.emit("inherited_return", map[string]any{"pid": pid, "ret": ret})
					t.newborn = false
				} else {
					row := map[string]any{"pid": pid, "nr": t.nr, "ret": ret}
					if lifecycleEnabled() {
						row["lifecycle_return"] = c.lifecycleOutput(pid, t, ret)
					}
					if messageCall(t.nr) {
						row["messages"] = c.messages(pid, t, true, ret)
					}
					c.emit("leave", row)
					t.entered = false
					c.release(pid)
				}
				c.resume(pid, 0)
			} else {
				if lifecycleEnabled() {
					c.issue(fmt.Sprintf("unsupported syscall stop %d", op))
					c.resume(pid, 0)
				} else {
					panic(fmt.Sprintf("unsupported syscall stop %d", op))
				}
			}
		} else if status.StopSignal() == syscall.SIGSTOP && t.newborn {
			c.emit("child_stop", map[string]any{"pid": pid})
			c.resume(pid, 0)
		} else {
			c.emit("signal", map[string]any{"pid": pid, "signal": int(status.StopSignal())})
			c.resume(pid, int(status.StopSignal()))
		}
	}
	if len(earlyStops) > 0 {
		c.issue("child stop without lineage")
	}
	c.snapshot("final", 0)
	c.emit("footer", map[string]any{"root_exit": rootExit, "issues": c.issues, "status": "unknown", "temp_peak_live_bytes": nil, "qualification_blocked": true})
}
