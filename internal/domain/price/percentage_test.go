package price_test

import (
	"errors"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
)

func TestPercentageThreshold(t *testing.T) {
	for _, tt := range []struct {
		base, percent, threshold, canonical string
		direction                           price.Direction
	}{
		{"200", "+5", "210", "+5", price.Up},
		{"200", "-5", "190", "-5", price.Down},
		{"0.1", "0.1", "0.1001", "+0.1", price.Up},
		{"100", "0.000000000000000001", "100.000000000000000001", "+0.000000000000000001", price.Up},
		{"100", "-0.000000000000000001", "99.999999999999999999", "-0.000000000000000001", price.Down},
		{"200", "+0005.00", "210", "+5", price.Up},
		{"1", "-99.99999999999999", "0.0000000000000001", "-99.99999999999999", price.Down},
		{"0.000000000000000001", "100", "0.000000000000000002", "+100", price.Up},
		{"500000000000000000", "99.9999999999999998", "999999999999999999", "+99.9999999999999998", price.Up},
	} {
		t.Run(tt.base+"/"+tt.percent, func(t *testing.T) {
			base, err := price.Parse(tt.base)
			if err != nil {
				t.Fatal(err)
			}
			got, err := price.NewPercentageThreshold(base, tt.percent)
			if err != nil {
				t.Fatal(err)
			}
			if got.Base.String() != tt.base || got.Percent != tt.canonical || got.Direction != tt.direction || got.Threshold.String() != tt.threshold {
				t.Fatalf("unexpected result: %+v", got)
			}
			if base.String() != tt.base {
				t.Fatal("base mutated")
			}
		})
	}
}

func TestPercentageThresholdRejectsInvalidInput(t *testing.T) {
	for _, tt := range []struct {
		name, base, percent string
		want                error
	}{
		{"zero base", "0", "5", price.ErrBase},
		{"zero percent", "200", "0", price.ErrPercent},
		{"negative zero", "200", "-0.00", price.ErrPercent},
		{"positive zero", "200", "+0", price.ErrPercent},
		{"total loss", "200", "-100", price.ErrPercent},
		{"greater loss", "200", "-100.000000000000000001", price.ErrPercent},
		{"large loss", "200", "-999999999999999999", price.ErrPercent},
		{"empty", "200", "", price.ErrFormat},
		{"sign only", "200", "-", price.ErrFormat},
		{"plus only", "200", "+", price.ErrFormat},
		{"double sign", "200", "--5", price.ErrFormat},
		{"mixed signs", "200", "+-5", price.ErrFormat},
		{"space", "200", " 5", price.ErrFormat},
		{"exponent", "200", "5e1", price.ErrFormat},
		{"comma", "200", "5,1", price.ErrFormat},
		{"percent symbol", "200", "5%", price.ErrFormat},
		{"missing integer", "200", ".5", price.ErrFormat},
		{"missing fraction", "200", "5.", price.ErrFormat},
		{"percent precision", "200", "0.0000000000000000001", price.ErrRange},
		{"percent trailing zeros", "200", "1.0000000000000000000", price.ErrRange},
		{"percent range", "200", "1000000000000000000", price.ErrRange},
		{"threshold precision", "1", "0.000000000000000001", price.ErrRange},
		{"tiny decrease", "0.000000000000000001", "-1", price.ErrRange},
		{"tiny increase", "0.000000000000000001", "1", price.ErrRange},
		{"threshold overflow", "999999999999999999.999999999999999999", "100", price.ErrRange},
	} {
		t.Run(tt.name, func(t *testing.T) {
			base, err := price.Parse(tt.base)
			if err != nil {
				t.Fatal(err)
			}
			got, err := price.NewPercentageThreshold(base, tt.percent)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %+v, error %v; want %v", got, err, tt.want)
			}
			if got.Base.Cmp(price.Price{}) != 0 || got.Threshold.Cmp(price.Price{}) != 0 || got.Percent != "" || got.Direction != "" {
				t.Fatalf("partial result on error: %+v", got)
			}
			if base.String() != tt.base {
				t.Fatal("base mutated on error")
			}
		})
	}
}

func TestPercentageThresholdSnapshot(t *testing.T) {
	base, err := price.Parse("200")
	if err != nil {
		t.Fatal(err)
	}
	result, err := price.NewPercentageThreshold(base, "+5")
	if err != nil {
		t.Fatal(err)
	}
	// Replacing the caller's latest price must not move the saved threshold/base.
	if err := base.UnmarshalText([]byte("300")); err != nil {
		t.Fatal(err)
	}
	if result.Base.String() != "200" || result.Threshold.String() != "210" {
		t.Fatalf("snapshot changed: %+v", result)
	}
	for i := 0; i < 8; i++ {
		t.Run("parallel calculation", func(t *testing.T) {
			t.Parallel()
			for j := 0; j < 100; j++ {
				got, err := price.NewPercentageThreshold(result.Base, "-5")
				if err != nil || got.Threshold.String() != "190" || result.Base.String() != "200" || result.Threshold.String() != "210" {
					t.Fatalf("calculation changed shared price: %+v, %v", got, err)
				}
			}
		})
	}
}
