package price

import (
	"errors"
	"math/big"
	"strings"
)

var (
	// ErrBase indicates a nonpositive creation price.
	ErrBase = errors.New("price: percentage base must be positive")
	// ErrPercent indicates zero change or a decrease of at least 100 percent.
	ErrPercent = errors.New("price: percent must be nonzero and greater than -100")
)

// Direction identifies the comparison associated with a fixed threshold.
type Direction string

const (
	Up   Direction = "up"
	Down Direction = "down"
)

// PercentageThreshold retains the creation snapshot and canonical signed percent.
// Callers persist these values together; no later market price enters this result.
type PercentageThreshold struct {
	Base      Price
	Percent   string
	Direction Direction
	Threshold Price
}

// NewPercentageThreshold computes base * (1 + percent/100) exactly.
// Percent accepts an optional + or - followed by the same decimal grammar and
// 18/18 input limits as Parse. Base must be positive; zero percent and decreases
// of 100% or more are rejected. The result must fit Price without rounding.
// Invalid decimal syntax returns ErrFormat, input/result range or precision
// violations return ErrRange. Any error returns an empty result.
func NewPercentageThreshold(base Price, percent string) (PercentageThreshold, error) {
	if base.Cmp(Price{}) <= 0 {
		return PercentageThreshold{}, ErrBase
	}
	direction, sign := Up, "+"
	magnitude := percent
	if strings.HasPrefix(percent, "-") {
		direction, sign, magnitude = Down, "-", percent[1:]
	} else if strings.HasPrefix(percent, "+") {
		magnitude = percent[1:]
	}
	change, err := Parse(magnitude)
	if err != nil {
		return PercentageThreshold{}, err
	}
	if change.Cmp(Price{}) == 0 {
		return PercentageThreshold{}, ErrPercent
	}

	// 100% at the percentage's scale. All receivers are fresh integers;
	// published Price coefficients (including base) remain immutable.
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(change.scale)+2), nil)
	if direction == Down {
		factor.Sub(factor, change.coefficientAtScale(change.scale))
		if factor.Sign() <= 0 {
			return PercentageThreshold{}, ErrPercent
		}
	} else {
		factor.Add(factor, change.coefficientAtScale(change.scale))
	}
	coefficient := new(big.Int).Mul(base.coefficientAtScale(base.scale), factor)
	scale := base.scale + change.scale + 2
	ten := big.NewInt(10)
	for scale > 0 && new(big.Int).Rem(coefficient, ten).Sign() == 0 {
		coefficient.Quo(coefficient, ten)
		scale--
	}
	// Normalize before enforcing precision: intermediate fractional zeros do not
	// lose information and must not cause a false range error.
	threshold, err := Parse((Price{coefficient: coefficient, scale: scale}).String())
	if err != nil {
		return PercentageThreshold{}, err
	}
	return PercentageThreshold{Base: base, Percent: sign + change.String(), Direction: direction, Threshold: threshold}, nil
}
