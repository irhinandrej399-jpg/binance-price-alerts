package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
)

// Linux arm64 LP64 user_msghdr: 56 bytes; mmsghdr: 64; cmsghdr: 16.
// Limits are explicit incomplete evidence, never an empty control buffer.
type memory struct {
	Address uint64 `json:"address"`
	Length  uint64 `json:"length"`
	Hex     string `json:"hex"`
	Error   string `json:"error"`
}
type right struct {
	FD    int    `json:"fd"`
	Stat  *file  `json:"stat"`
	Error string `json:"error"`
}
type message struct {
	Header  memory  `json:"header"`
	Control *memory `json:"control"`
	Rights  []right `json:"rights"`
	Error   string  `json:"error"`
}
type messages struct {
	Version string    `json:"version"`
	Items   []message `json:"items"`
	Error   string    `json:"error"`
}

func messageCall(n uint64) bool { return n == 211 || n == 212 || n == 243 || n == 269 }
func receive(n uint64) bool     { return n == 212 || n == 243 }
func multi(n uint64) bool       { return n == 243 || n == 269 }
func readMemory(pid int, address, length uint64) memory {
	m := memory{Address: address, Length: length}
	if length > 1<<20 || address > 1<<63-1 || address+length < address {
		m.Error = "memory range exceeds capture limit"
		return m
	}
	if length == 0 {
		return m
	}
	f, e := os.Open(fmt.Sprintf("/proc/%d/mem", pid))
	if e != nil {
		m.Error = e.Error()
		return m
	}
	defer f.Close()
	b := make([]byte, int(length))
	n, e := f.ReadAt(b, int64(address))
	m.Hex = hex.EncodeToString(b[:n])
	if e != nil {
		m.Error = e.Error()
	}
	return m
}
func (c *capture) messages(pid int, t *task, output bool, ret int64) messages {
	out := messages{Version: "linux-arm64-ancillary-v1", Items: []message{}}
	count := uint64(1)
	stride := uint64(56)
	if multi(t.nr) {
		count = t.args[2]
		stride = 64
	}
	if output {
		if ret < 0 {
			return out
		}
		if multi(t.nr) {
			count = uint64(ret)
		}
	}
	if count > 1024 || (count > 0 && t.args[1]+(count-1)*stride < t.args[1]) {
		out.Error = "message vector exceeds 1024"
		c.issue(out.Error)
		return out
	}
	for i := uint64(0); i < count; i++ {
		m := message{Header: readMemory(pid, t.args[1]+i*stride, stride), Rights: []right{}}
		if m.Header.Error != "" {
			out.Items = append(out.Items, m)
			c.issue("message header unreadable")
			continue
		}
		b, _ := hex.DecodeString(m.Header.Hex)
		ptr := binary.LittleEndian.Uint64(b[32:40])
		length := binary.LittleEndian.Uint64(b[40:48])
		// Receive input is capacity, not initialized ancillary bytes. Failed syscall
		// outputs are not decoded. Send input includes even attempted/unsent entries.
		if output && receive(t.nr) && binary.LittleEndian.Uint32(b[48:52])&8 != 0 {
			c.issue("MSG_CTRUNC: discarded references unresolved")
		}
		if receive(t.nr) == output {
			control := readMemory(pid, ptr, length)
			m.Control = &control
			if control.Error != "" {
				m.Error = "control unreadable"
			} else {
				data, _ := hex.DecodeString(control.Hex)
				for off := 0; off+16 <= len(data); {
					size := binary.LittleEndian.Uint64(data[off : off+8])
					level := binary.LittleEndian.Uint32(data[off+8 : off+12])
					typ := binary.LittleEndian.Uint32(data[off+12 : off+16])
					if size < 16 || size > uint64(len(data)-off) {
						m.Error = "invalid cmsghdr length"
						break
					}
					if level == 1 && typ == 1 {
						if (size-16)%4 != 0 {
							m.Error = "invalid rights length"
							break
						}
						for j := off + 16; j < off+int(size); j += 4 {
							fd := int(int32(binary.LittleEndian.Uint32(data[j : j+4])))
							r := right{FD: fd}
							f, e := stat(fmt.Sprintf("/proc/%d/fd/%d", pid, fd))
							if e != nil {
								r.Error = e.Error()
							} else {
								f.PID = pid
								f.FD = fd
								f.Path, _ = os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", pid, fd))
								r.Stat = &f
							}
							if r.Error != "" {
								c.issue("message FD stat unreadable")
							}
							m.Rights = append(m.Rights, r)
						}
					} else {
						m.Error = "ancillary type has unresolved reference semantics"
					}
					off += (int(size) + 7) &^ 7
				}
			}
		}
		if m.Error != "" {
			c.issue(m.Error)
		}
		out.Items = append(out.Items, m)
	}
	return out
}
