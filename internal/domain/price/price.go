// Package price provides exact, nonnegative decimal prices without rounding.
// Values have at most 18 integer digits and 18 fractional digits. The zero
// value is 0. Copies share no mutable state; unmarshaling replaces the receiver.
package price

import (
	"encoding/json"
	"errors"
	"strings"
)

var (
	// ErrFormat indicates a decimal syntax violation.
	ErrFormat = errors.New("price: expected unsigned decimal digits with optional fraction")
	// ErrRange indicates an integer range or fractional precision violation.
	ErrRange = errors.New("price: exceeds 18 integer or 18 fractional digits")
)

// Price stores a canonical decimal; an empty internal string represents zero.
type Price struct{ value string }

// Parse accepts [0-9]+(\.[0-9]+)?. Leading integer zeros are ignored when
// checking the range. More than 18 input fractional digits are rejected,
// including trailing zeros. No exponent, sign or whitespace is accepted.
func Parse(s string) (Price, error) {
	integer, fraction, dot := strings.Cut(s, ".")
	if !digits(integer) || (dot && !digits(fraction)) {
		return Price{}, ErrFormat
	}
	integer = strings.TrimLeft(integer, "0")
	if len(integer) > 18 || len(fraction) > 18 {
		return Price{}, ErrRange
	}
	if integer == "" {
		integer = "0"
	}
	fraction = strings.TrimRight(fraction, "0")
	if fraction != "" {
		return Price{integer + "." + fraction}, nil
	}
	if integer == "0" {
		return Price{}, nil
	}
	return Price{integer}, nil
}

func digits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// String returns the exact value without redundant leading or trailing zeros.
func (p Price) String() string {
	if p.value == "" {
		return "0"
	}
	return p.value
}

// Cmp returns -1, 0 or 1 when p is less than, equal to or greater than other.
func (p Price) Cmp(other Price) int {
	a, af, _ := strings.Cut(p.String(), ".")
	b, bf, _ := strings.Cut(other.String(), ".")
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	if c := strings.Compare(a, b); c != 0 {
		return c
	}
	// Missing decimal places have value zero, not an ordering by string length.
	for i := 0; i < max(len(af), len(bf)); i++ {
		ac, bc := byte('0'), byte('0')
		if i < len(af) {
			ac = af[i]
		}
		if i < len(bf) {
			bc = bf[i]
		}
		if ac < bc {
			return -1
		}
		if ac > bc {
			return 1
		}
	}
	return 0
}

// MarshalText serializes the canonical exact decimal value.
func (p Price) MarshalText() ([]byte, error) { return []byte(p.String()), nil }

// UnmarshalText replaces p only after successful validation.
func (p *Price) UnmarshalText(text []byte) error {
	value, err := Parse(string(text))
	if err != nil {
		return err
	}
	*p = value
	return nil
}

// MarshalJSON uses a JSON string so consumers cannot lose decimal precision
// by decoding a JSON number into a binary floating-point value.
func (p Price) MarshalJSON() ([]byte, error) { return json.Marshal(p.String()) }

// UnmarshalJSON accepts only a JSON string containing a valid price.
func (p *Price) UnmarshalJSON(data []byte) error {
	var s *string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == nil {
		return ErrFormat
	}
	return p.UnmarshalText([]byte(*s))
}
