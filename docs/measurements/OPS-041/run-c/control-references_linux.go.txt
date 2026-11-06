package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

func referenceControls() {
	runtime.LockOSThread()
	check(os.MkdirAll("/tmp/work", 0700))
	read := func(p string) string { b, e := os.ReadFile(p); check(e); return string(b) }
	point := func(tag string, s map[string]any, sp [2]int, queued int, mapped bool) {
		fds, e := os.ReadDir("/proc/self/fd")
		check(e)
		entries := []any{}
		for _, f := range fds {
			target, e := os.Readlink("/proc/self/fd/" + f.Name())
			entries = append(entries, map[string]any{"fd": f.Name(), "target": target, "error": fmt.Sprint(e)})
		}
		check(json.NewEncoder(os.Stdout).Encode(map[string]any{"tag": tag, "pid": os.Getpid(), "tid": syscall.Gettid(), "source": s, "sockets": []any{evidence(sp[0]), evidence(sp[1])}, "queued_bytes": queued, "mapped": mapped, "fds": entries, "maps": read("/proc/self/maps"), "status": read(fmt.Sprintf("/proc/self/task/%d/status", syscall.Gettid())), "stat": read(fmt.Sprintf("/proc/self/task/%d/stat", syscall.Gettid()))}))
		_, e = syscall.Getpgid(0)
		check(e)
	}
	queued := func(fd int) int {
		var n int32
		_, _, e := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), 0x541b, uintptr(unsafe.Pointer(&n)))
		if e != 0 {
			panic(e)
		}
		return int(n)
	}
	for _, mode := range []string{"receive", "drop", "mapping"} {
		sp, e := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_DGRAM|syscall.SOCK_CLOEXEC, 0)
		check(e)
		fd, e := syscall.Open("/tmp/work/reused", syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR|syscall.O_CLOEXEC, 0600)
		check(e)
		check(syscall.Ftruncate(fd, 8192))
		s := evidence(fd)
		point(mode+"-opened", s, sp, 0, false)
		check(syscall.Unlink("/tmp/work/reused"))
		s = evidence(fd)
		if mode == "mapping" {
			b, e := syscall.Mmap(fd, 0, 8192, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
			check(e)
			check(syscall.Close(fd))
			b[0] = 42
			point("mapping-only", s, sp, 0, true)
			if b[0] != 42 {
				panic("mapping lost")
			}
			check(syscall.Munmap(b))
			point("mapping-unmapped", s, sp, 0, false)
		} else {
			n, e := syscall.SendmsgN(sp[0], []byte{'x'}, syscall.UnixRights(fd), nil, 0)
			check(e)
			if n != 1 {
				panic("send length")
			}
			check(syscall.Close(fd))
			point(mode+"-queue-only", s, sp, queued(sp[1]), false)
			if mode == "receive" {
				b, o := make([]byte, 1), make([]byte, 64)
				n, on, flags, _, e := syscall.Recvmsg(sp[1], b, o, 0)
				check(e)
				if n != 1 || b[0] != 'x' || flags != 0 {
					panic("receive payload/flags")
				}
				msgs, e := syscall.ParseSocketControlMessage(o[:on])
				check(e)
				if len(msgs) != 1 {
					panic("control count")
				}
				fs, e := syscall.ParseUnixRights(&msgs[0])
				check(e)
				if len(fs) != 1 {
					panic("rights count")
				}
				point("receive-delivered", evidence(fs[0]), sp, queued(sp[1]), false)
				check(syscall.Close(fs[0]))
			} else {
				// Zero control capacity really drops the queued right via MSG_CTRUNC.
				b := make([]byte, 1)
				n, on, flags, _, e := syscall.Recvmsg(sp[1], b, nil, 0)
				check(e)
				if n != 1 || on != 0 || flags&syscall.MSG_CTRUNC == 0 {
					panic("drop not truncated")
				}
			}
			point(mode+"-discharged", s, sp, queued(sp[1]), false)
		}
		// Same pathname and exact FD number; identity is independently fstat'ed.
		next, e := syscall.Open("/tmp/work/reused", syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR|syscall.O_CLOEXEC, 0600)
		check(e)
		if next != fd {
			check(syscall.Dup3(next, fd, syscall.O_CLOEXEC))
			check(syscall.Close(next))
		}
		check(syscall.Ftruncate(fd, 4096))
		point(mode+"-reused", evidence(fd), sp, 0, false)
		check(syscall.Close(fd))
		check(syscall.Unlink("/tmp/work/reused"))
		check(syscall.Close(sp[0]))
		check(syscall.Close(sp[1]))
	}
}
