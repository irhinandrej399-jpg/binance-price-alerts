package apiprocess

import (
	"errors"
	"strconv"
	"strings"
)

// exec serializes writes and Wait joins the copier before closing Process.done.
// Keep only a bounded line and one bit per trusted category, never raw output.
type diagnosticSink struct {
	allowed  []string
	seen     []bool
	line     []byte
	overflow bool
}

func newDiagnosticSink(lines []string) (*diagnosticSink, error) {
	if len(lines) > 32 {
		return nil, ErrConfig
	}
	for _, line := range lines {
		if line == "" || len(line) > 256 || strings.ContainsAny(line, "\r\n") {
			return nil, ErrConfig
		}
	}
	return &diagnosticSink{allowed: append([]string(nil), lines...), seen: make([]bool, len(lines))}, nil
}
func (d *diagnosticSink) Write(b []byte) (int, error) {
	for _, c := range b {
		if c == '\n' {
			if !d.overflow {
				for i, line := range d.allowed {
					if string(d.line) == line {
						d.seen[i] = true
					}
				}
			}
			d.line = d.line[:0]
			d.overflow = false
		} else if len(d.line) < 256 && !d.overflow {
			d.line = append(d.line, c)
		} else {
			d.overflow = true
			d.line = d.line[:0]
		}
	}
	return len(b), nil
}
func earlyExit(p *Process, d *diagnosticSink) error {
	code, _ := p.ExitCode()
	result := errors.Join(ErrEarlyExit, errors.New("exit status "+strconv.Itoa(code)))
	for i, line := range d.allowed {
		if d.seen[i] {
			result = errors.Join(result, errors.New(line))
		}
	}
	return result
}
