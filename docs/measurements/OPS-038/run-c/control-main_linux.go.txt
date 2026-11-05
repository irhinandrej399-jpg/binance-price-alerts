// Independent fstat/procfs witness helper; never uses the tracer inventory.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func read(p string) string  { b, e := os.ReadFile(p); check(e); return string(b) }
func emit(r map[string]any) { check(json.NewEncoder(os.Stdout).Encode(r)) }
func source(fd int) map[string]any {
	var s syscall.Stat_t
	check(syscall.Fstat(fd, &s))
	return map[string]any{"fd": fd, "dev": s.Dev, "ino": s.Ino, "mode": s.Mode, "size": s.Size, "nlink": s.Nlink}
}
func point(tag string, fd int, saved map[string]any) {
	s := saved
	if fd >= 0 {
		s = source(fd)
	}
	es, e := os.ReadDir("/proc/self/task")
	check(e)
	tasks := []any{}
	for _, t := range es {
		base := "/proc/self/task/" + t.Name()
		tasks = append(tasks, map[string]any{"tid": t.Name(), "stat": read(base + "/stat"), "status": read(base + "/status")})
	}
	emit(map[string]any{"tag": tag, "pid": os.Getpid(), "tid": syscall.Gettid(), "source": s, "tasks": tasks, "maps": read("/proc/self/maps")})
	// This syscall is gated only in OPS-038 mode. Main thread makes no reference
	// mutation between fstat/maps witness and this stop. Other owners remain live.
	_, e = syscall.Getpgid(0)
	check(e)
}
func main() {
	runtime.LockOSThread()
	if len(os.Args) > 1 && os.Args[1] == "external" {
		fd, e := syscall.Open("/probe/external", syscall.O_CREAT|syscall.O_RDWR|syscall.O_EXCL, 0600)
		check(e)
		check(syscall.Ftruncate(fd, 12288))
		emit(map[string]any{"tag": "external", "pid": os.Getpid(), "source": source(fd), "stat": read("/proc/self/stat")})
		check(os.WriteFile("/probe/ready", []byte(strconv.Itoa(os.Getpid())), 0600))
		for {
			if _, e := os.Stat("/probe/stop"); e == nil {
				break
			}
			time.Sleep(time.Millisecond * 10)
		}
		check(syscall.Close(fd))
		return
	}
	check(os.MkdirAll("/probe/work", 0700))
	fd, e := syscall.Open("/probe/work/source", syscall.O_CREAT|syscall.O_RDWR|syscall.O_EXCL, 0600)
	check(e)
	check(syscall.Ftruncate(fd, 4096))
	point("before-rename", fd, nil)
	check(syscall.Rename("/probe/work/source", "/probe/outside"))
	point("renamed", fd, nil)
	check(syscall.Ftruncate(fd, 8192))
	point("grown", fd, nil)
	check(syscall.Unlink("/probe/outside"))
	point("unlinked", fd, nil)
	mapping, e := syscall.Mmap(fd, 0, 8192, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	check(e)
	saved := source(fd)
	check(syscall.Close(fd))
	mapping[0] = 42
	point("mapping-only", -1, saved)
	check(syscall.Munmap(mapping))
	point("unmapped", -1, saved)
	// Independent process not in ptrace lineage disappears between checkpoints.
	external := read("/probe/ready")
	check(os.WriteFile("/probe/stop", []byte("stop"), 0600))
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, e := os.Stat(filepath.Join("/proc", external)); os.IsNotExist(e) {
			break
		}
		if time.Now().After(deadline) {
			panic("external owner did not exit")
		}
		time.Sleep(time.Millisecond * 10)
	}
	point("external-gone", -1, saved)
	fmt.Println(`{"tag":"complete"}`)
}
