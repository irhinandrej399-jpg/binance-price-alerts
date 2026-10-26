package price

import (
	"encoding/json"
	"errors"
	"math/big"
	"testing"
)

func TestCmpMatchesRational(t *testing.T) {
	values := []string{
		"0", "0.000000000000000001", "0.1", "0.100000000000000001",
		"0.99", "1", "1.01", "1.1", "9", "10", "123.45",
		"999999999999999999", "999999999999999999.999999999999999999",
	}
	for _, a := range values {
		for _, b := range values {
			p, err := Parse(a)
			if err != nil {
				t.Fatal(err)
			}
			q, err := Parse(b)
			if err != nil {
				t.Fatal(err)
			}
			ar, _ := new(big.Rat).SetString(a)
			br, _ := new(big.Rat).SetString(b)
			for range 3 {
				if got, want := p.Cmp(q), ar.Cmp(br); got != want {
					t.Fatalf("%s Cmp %s = %d, want %d", a, b, got, want)
				}
			}
			if p.String() != a || q.String() != b {
				t.Fatal("Cmp changed its operands")
			}
		}
	}
}

func TestCopiesRemainIndependent(t *testing.T) {
	original, err := Parse("999999999999999999.999999999999999999")
	if err != nil {
		t.Fatal(err)
	}
	copy := original
	if err := copy.UnmarshalText([]byte("0.1")); err != nil {
		t.Fatal(err)
	}
	if original.String() != "999999999999999999.999999999999999999" {
		t.Fatal("unmarshal changed original")
	}
	for range 8 {
		t.Run("concurrent reads", func(t *testing.T) {
			t.Parallel()
			for range 100 {
				if original.Cmp(copy) != 1 || copy.Cmp(original) != -1 {
					t.Fatal("incorrect comparison")
				}
				if copy.String() != "0.1" {
					t.Fatal("comparison changed copy")
				}
			}
		})
	}
}

func TestSerialization(t *testing.T) {
	for _, input := range []string{"0", "0.1", "00123.4500", "0.000000000000000001", "999999999999999999.999999999999999999"} {
		p, err := Parse(input)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != `"`+p.String()+`"` {
			t.Fatalf("JSON = %s", encoded)
		}
		var decoded Price
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Cmp(p) != 0 {
			t.Fatalf("JSON round trip changed %s to %s", p, decoded)
		}
		text, err := p.MarshalText()
		if err != nil || string(text) != p.String() {
			t.Fatalf("text = %q, error = %v", text, err)
		}
		if err := decoded.UnmarshalText(text); err != nil {
			t.Fatal(err)
		}
		if decoded.Cmp(p) != 0 {
			t.Fatal("text round trip changed value")
		}
	}
	var zero Price
	encoded, err := json.Marshal(zero)
	if err != nil || string(encoded) != `"0"` {
		t.Fatalf("zero JSON = %s, %v", encoded, err)
	}
}

func TestUnmarshalValidation(t *testing.T) {
	for _, input := range []string{`null`, `0.1`, `true`, `{}`, `[]`, `"-1"`, `"1e2"`, `"0.0000000000000000001"`, `"1000000000000000000"`, `""`, `"1`, `"1" "2"`} {
		p, _ := Parse("7.5")
		if err := json.Unmarshal([]byte(input), &p); err == nil {
			t.Errorf("accepted JSON %s", input)
		}
		if p.String() != "7.5" {
			t.Errorf("failed JSON %s changed value to %s", input, p)
		}
	}
	p, _ := Parse("7.5")
	if err := p.UnmarshalText([]byte("-1")); !errors.Is(err, ErrFormat) {
		t.Fatalf("text error = %v", err)
	}
	if p.String() != "7.5" {
		t.Fatal("failed text unmarshal changed value")
	}
	if err := json.Unmarshal([]byte(`"0001.2000"`), &p); err != nil || p.String() != "1.2" {
		t.Fatalf("valid JSON: %s, %v", p, err)
	}
}

func TestParse(t *testing.T) {
	for _, tt := range []struct {
		input, want string
		err         error
	}{
		{"0.1", "0.1", nil},
		{"0", "0", nil},
		{"000.000", "0", nil},
		{"00123.4500", "123.45", nil},
		{"0.123456789012345678", "0.123456789012345678", nil},
		{"0.000000000000000001", "0.000000000000000001", nil},
		{"999999999999999999.999999999999999999", "999999999999999999.999999999999999999", nil},
		{"1000000000000000000", "", ErrRange},
		{"0.1234567890123456789", "", ErrRange},
		{"1.0000000000000000000", "", ErrRange},
		{"-1", "", ErrFormat},
		{"-0", "", ErrFormat},
		{"", "", ErrFormat},
		{".1", "", ErrFormat},
		{"1.", "", ErrFormat},
		{"1.2.3", "", ErrFormat},
		{"+1", "", ErrFormat},
		{" 1", "", ErrFormat},
		{"1\n", "", ErrFormat},
		{"1e3", "", ErrFormat},
		{"1,2", "", ErrFormat},
		{"NaN", "", ErrFormat},
		{"Inf", "", ErrFormat},
		{"１", "", ErrFormat},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input)
			if !errors.Is(err, tt.err) {
				t.Fatalf("Parse: error = %v, want %v", err, tt.err)
			}
			if err == nil && got.String() != tt.want {
				t.Fatalf("got %q, want %q", got.String(), tt.want)
			}
		})
	}
}

func TestCmp(t *testing.T) {
	for _, tt := range []struct {
		a, b string
		want int
	}{
		{"0.1", "0.1000", 0},
		{"9", "10", -1},
		{"1.01", "1.1", -1},
		{"0", "0.000000000000000001", -1},
		{"999999999999999999.999999999999999998", "999999999999999999.999999999999999999", -1},
	} {
		a, err := Parse(tt.a)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Parse(tt.b)
		if err != nil {
			t.Fatal(err)
		}
		if got := a.Cmp(b); got != tt.want {
			t.Errorf("%s Cmp %s = %d, want %d", a, b, got, tt.want)
		}
		if got := b.Cmp(a); got != -tt.want {
			t.Errorf("reverse Cmp = %d, want %d", got, -tt.want)
		}
	}
	var zero Price
	parsed, _ := Parse("0")
	if zero.String() != "0" || zero.Cmp(parsed) != 0 {
		t.Fatal("zero value must represent 0")
	}
}
