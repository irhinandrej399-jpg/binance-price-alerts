// Package price provides exact, nonnegative decimal prices without rounding.
// Values have at most 18 integer digits and 18 fractional digits. The zero
// value is 0. Copies share only immutable data; unmarshaling replaces the receiver.
package price

import (
	"encoding/json"
	"errors"
	"math/big"
	"strings"
)

var (
	// ErrFormat indicates a decimal syntax violation.
	ErrFormat = errors.New("price: expected unsigned decimal digits with optional fraction")
	// ErrRange indicates an integer range or fractional precision violation.
	ErrRange = errors.New("price: exceeds 18 integer or 18 fractional digits")
)

// Price represents coefficient / 10^scale. A nil coefficient represents zero.
// The coefficient is private and never modified after construction or exposed
// to callers. Arithmetic must use a fresh big.Int receiver to keep copies safe.
// Use Cmp to compare values, not Go's pointer-based struct equality.
type Price struct {
	coefficient *big.Int
	scale       uint8
}

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
	if integer == "0" && fraction == "" {
		return Price{}, nil
	}
	// Both parts have already been validated as decimal digits, so SetString
	// cannot fail. Trailing fractional zeros were removed to normalize the scale.
	coefficient, _ := new(big.Int).SetString(integer+fraction, 10)
	return Price{coefficient: coefficient, scale: uint8(len(fraction))}, nil
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
	if p.coefficient == nil {
		return "0"
	}
	digits := p.coefficient.String()
	scale := int(p.scale)
	if scale == 0 {
		return digits
	}
	if len(digits) <= scale {
		return "0." + strings.Repeat("0", scale-len(digits)) + digits
	}
	point := len(digits) - scale
	return digits[:point] + "." + digits[point:]
}

// Cmp returns -1, 0 or 1 when p is less than, equal to or greater than other.
func (p Price) Cmp(other Price) int {
	scale := max(p.scale, other.scale)
	return p.coefficientAtScale(scale).Cmp(other.coefficientAtScale(scale))
}

// coefficientAtScale returns an independent integer at a scale >= p.scale.
// Using big.Int avoids overflow even when alignment exceeds the price range.
func (p Price) coefficientAtScale(scale uint8) *big.Int {
	coefficient := new(big.Int)
	if p.coefficient != nil {
		coefficient.Set(p.coefficient)
	}
	if scale > p.scale {
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale-p.scale)), nil)
		coefficient.Mul(coefficient, factor)
	}
	return coefficient
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
