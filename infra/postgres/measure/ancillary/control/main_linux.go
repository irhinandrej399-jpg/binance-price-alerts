// Real Linux arm64 SCM_RIGHTS controls; requires a disposable /tmp/work.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"
	"unsafe"
)

var controls = map[uint64][]byte{}

type hdr struct {
	Name       uint64
	Namelen    uint32
	Pad        uint32
	Iov        uint64
	Iovlen     uint64
	Control    uint64
	Controllen uint64
	Flags      uint32
	Pad2       uint32
}
type mhdr struct {
	H   hdr
	Len uint32
	Pad uint32
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func ptr[T any](p *T) uint64 { return uint64(uintptr(unsafe.Pointer(p))) }
func evidence(fd int) map[string]any {
	var s syscall.Stat_t
	check(syscall.Fstat(fd, &s))
	return map[string]any{"fd": fd, "dev": s.Dev, "ino": s.Ino, "size": s.Size, "nlink": s.Nlink, "mode": s.Mode}
}
func call(tag string, nr uintptr, socket int, hs []mhdr, flags uintptr) {
	// Single msghdr shares its initial layout with mmsghdr.
	ret, _, err := syscall.Syscall6(nr, uintptr(socket), uintptr(unsafe.Pointer(&hs[0])), func() uintptr {
		if nr == 211 || nr == 212 {
			return flags
		}
		return uintptr(len(hs))
	}(), func() uintptr {
		if nr == 211 || nr == 212 {
			return 0
		}
		return flags
	}(), 0, 0)
	row := map[string]any{"pid": os.Getpid(), "tag": tag, "nr": nr, "ret": int64(ret), "errno": int(err), "messages": []any{}}
	out := []any{}
	count := len(hs)
	if nr == 243 || nr == 269 {
		if err == 0 {
			count = int(ret)
		} else {
			count = 0
		}
	}
	if err == 0 {
		for i := 0; i < count; i++ {
			h := hs[i].H
			fds := []any{}
			if nr == 212 || nr == 243 {
				if h.Controllen >= 16 {
					b := controls[h.Control][:h.Controllen]
					msgs, e := syscall.ParseSocketControlMessage(b)
					check(e)
					for _, m := range msgs {
						fs, e := syscall.ParseUnixRights(&m)
						check(e)
						for _, fd := range fs {
							fds = append(fds, evidence(fd))
							defer syscall.Close(fd)
						}
					}
				}
			}
			out = append(out, map[string]any{"length": hs[i].Len, "flags": h.Flags, "controllen": h.Controllen, "rights": fds})
		}
	}
	row["messages"] = out
	check(json.NewEncoder(os.Stdout).Encode(row))
	runtime.KeepAlive(hs)
}
func main() {
	runtime.LockOSThread()
	if len(os.Args) > 1 && os.Args[1] == "receive" {
		b := []byte{'x'}
		iov := syscall.Iovec{Base: &b[0], Len: 1}
		o := make([]byte, 64)
		h := mhdr{H: hdr{Iov: ptr(&iov), Iovlen: 1, Control: ptr(&o[0]), Controllen: 64}}
		controls[h.H.Control] = o
		call("cross-process", 212, 3, []mhdr{h}, 0)
		runtime.KeepAlive([]any{b, iov, o})
		return
	}
	check(os.MkdirAll("/tmp/work", 0700))
	f, e := os.OpenFile("/tmp/work/ordinary", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	check(e)
	defer f.Close()
	check(f.Truncate(4096))
	g, e := os.OpenFile("/tmp/work/unlinked", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	check(e)
	defer g.Close()
	check(g.Truncate(8192))
	check(os.Remove(g.Name()))
	check(json.NewEncoder(os.Stdout).Encode(map[string]any{"cross_process": true, "pid": os.Getpid(), "source": []any{evidence(int(f.Fd())), evidence(int(g.Fd()))}, "abi": []uintptr{unsafe.Sizeof(hdr{}), unsafe.Sizeof(mhdr{}), unsafe.Sizeof(syscall.Cmsghdr{})}}))
	sp, e := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_DGRAM, 0)
	check(e)
	defer syscall.Close(sp[0])
	defer syscall.Close(sp[1])
	b := []byte{'x'}
	iov := syscall.Iovec{Base: &b[0], Len: 1}
	build := func(control []byte) mhdr {
		h := mhdr{H: hdr{Iov: ptr(&iov), Iovlen: 1, Controllen: uint64(len(control))}}
		if len(control) > 0 {
			h.H.Control = ptr(&control[0])
			controls[h.H.Control] = control
		}
		return h
	}
	send := func(tag string, fds ...int) {
		o := syscall.UnixRights(fds...)
		call(tag, 211, sp[0], []mhdr{build(o)}, 0)
		runtime.KeepAlive(o)
	}
	recv := func(tag string, size int) {
		o := make([]byte, size)
		call(tag, 212, sp[1], []mhdr{build(o)}, 0)
		runtime.KeepAlive(o)
	}
	send("ordinary", int(f.Fd()))
	recv("ordinary", 64)
	send("unlink", int(g.Fd()))
	recv("unlink", 64)
	send("multiple", int(f.Fd()), int(g.Fd()))
	recv("multiple", 64)
	call("no-control", 211, sp[0], []mhdr{build(nil)}, 0)
	recv("no-control", 64)
	send("truncated", int(f.Fd()), int(g.Fd()))
	recv("truncated", 20)
	send("zero-capacity", int(g.Fd()))
	recv("zero-capacity", 0)
	call("bad-socket", 211, -1, []mhdr{build(nil)}, 0)
	call("bad-header", 211, sp[0], []mhdr{{H: hdr{Iov: 1, Iovlen: 1}}}, 0)
	_, _, bad := syscall.Syscall6(211, uintptr(sp[0]), 1, 0, 0, 0, 0)
	if bad != syscall.EFAULT {
		panic(bad)
	}
	send("bad-right", -1)
	rights := syscall.UnixRights(int(g.Fd()))
	badrights := syscall.UnixRights(-1)
	call("partial-send", 269, sp[0], []mhdr{build(rights), build(badrights)}, 0)
	r1, r2 := make([]byte, 64), make([]byte, 64)
	call("partial-recv", 243, sp[1], []mhdr{build(r1), build(r2)}, syscall.MSG_DONTWAIT)
	call("full-send", 269, sp[0], []mhdr{build(rights), build(rights)}, 0)
	call("full-recv", 243, sp[1], []mhdr{build(r1), build(r2)}, syscall.MSG_DONTWAIT)
	call("empty-recv", 243, sp[1], []mhdr{build(r1), build(r2)}, syscall.MSG_DONTWAIT)
	// Force a blocking recvmsg to cross a real restart stop. Go installs
	// SA_RESTART; a peer goroutine sends SIGUSR1 before queuing the datagram.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		check(syscall.Kill(os.Getpid(), syscall.SIGUSR1))
		time.Sleep(100 * time.Millisecond)
		check(syscall.Sendmsg(sp[0], b, nil, nil, 0))
	}()
	recv("restart", 64)
	signal.Stop(ch)
	// Keep pointer-backed storage alive through the raw syscalls.
	runtime.KeepAlive([]any{b, iov, rights, badrights, r1, r2})
	// The receiver starts without the source FD, then gets the unlink-open
	// reference over a real inherited socket in another process.
	dup, e := syscall.Dup(sp[1])
	check(e)
	socket := os.NewFile(uintptr(dup), "receiver-socket")
	child := exec.Command(os.Args[0], "receive")
	child.ExtraFiles = []*os.File{socket}
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	check(child.Start())
	check(socket.Close())
	send("cross-process", int(g.Fd()))
	check(child.Wait())
	check(os.Remove(f.Name()))
	fmt.Println(`{"closed":true}`)
}
