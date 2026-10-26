package price_test

import (
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
)

func TestSatisfied(t *testing.T) {
	for _, tt := range []struct {
		name, current, threshold string
		direction                price.Direction
		want                     bool
	}{
		{"up below", "99", "100", price.Up, false},
		{"up equal", "100", "100", price.Up, true},
		{"up above on first price", "101", "100", price.Up, true},
		{"down above", "101", "100", price.Down, false},
		{"down equal", "100", "100", price.Down, true},
		{"down below on first price", "99", "100", price.Down, true},
		{"up smallest difference below", "0.1", "0.100000000000000001", price.Up, false},
		{"up smallest difference above", "0.100000000000000001", "0.1", price.Up, true},
		{"down smallest difference above", "999999999999999999.999999999999999999", "999999999999999999.999999999999999998", price.Down, false},
		{"down smallest difference below", "999999999999999999.999999999999999998", "999999999999999999.999999999999999999", price.Down, true},
		{"equal different scales", "100.00", "100", price.Down, true},
		{"empty direction", "100", "100", "", false},
		{"unknown direction", "100", "100", "sideways", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			current, err := price.Parse(tt.current)
			if err != nil {
				t.Fatal(err)
			}
			threshold, err := price.Parse(tt.threshold)
			if err != nil {
				t.Fatal(err)
			}
			if got := price.Satisfied(current, threshold, tt.direction); got != tt.want {
				t.Fatalf("Satisfied(%s, %s, %q) = %v, want %v", current, threshold, tt.direction, got, tt.want)
			}
		})
	}
}

func TestSatisfiedReevaluatesEachPrice(t *testing.T) {
	threshold, err := price.Parse("100")
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name      string
		direction price.Direction
		prices    []string
		want      []bool
	}{
		{"up equality", price.Up, []string{"99", "100"}, []bool{false, true}},
		{"down equality", price.Down, []string{"101", "100"}, []bool{false, true}},
		{"up jump", price.Up, []string{"99", "101"}, []bool{false, true}},
		{"down jump", price.Down, []string{"101", "99"}, []bool{false, true}},
		{"up remains false", price.Up, []string{"98", "99"}, []bool{false, false}},
		{"down remains false", price.Down, []string{"102", "101"}, []bool{false, false}},
		{"up creation condition disappears and returns", price.Up, []string{"101", "101", "99", "100"}, []bool{true, true, false, true}},
		{"down creation condition disappears and returns", price.Down, []string{"99", "99", "101", "100"}, []bool{true, true, false, true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for i, value := range tt.prices {
				current, err := price.Parse(value)
				if err != nil {
					t.Fatal(err)
				}
				if got := price.Satisfied(current, threshold, tt.direction); got != tt.want[i] {
					t.Fatalf("price[%d]=%s: got %v, want %v", i, value, got, tt.want[i])
				}
			}
		})
	}
}
