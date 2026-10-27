package password_test

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/password"
)

func TestHashAndVerify(t *testing.T) {
	for _, plain := range []string{"correct horse battery staple", " пробелы и Unicode 🔑 \x00", strings.Repeat("long", 100)} {
		encoded, err := password.Hash(plain)
		if err != nil {
			t.Fatal(err)
		}
		if err := password.Verify(encoded, plain); err != nil {
			t.Fatal(err)
		}
		for _, wrong := range []string{plain + "x", strings.TrimSpace(plain) + "wrong", ""} {
			if err := password.Verify(encoded, wrong); !errors.Is(err, password.ErrMismatch) {
				t.Fatal("wrong password accepted or unexpected error")
			}
		}
		second, err := password.Hash(plain)
		if err != nil {
			t.Fatal(err)
		}
		parts, other := strings.Split(encoded, "$"), strings.Split(second, "$")
		if len(parts) != 6 || parts[1] != "argon2id" || parts[2] != "v=19" || parts[3] != "m=19456,t=2,p=1" {
			t.Fatal("unexpected encoding or parameters")
		}
		salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
		if err != nil || len(salt) != 16 || parts[4] == other[4] || encoded == second {
			t.Fatal("salt must be fresh and 128 bits")
		}
		key, err := base64.RawStdEncoding.Strict().DecodeString(parts[5])
		if err != nil || len(key) != 32 {
			t.Fatal("expected 256-bit key")
		}
	}
}

func TestMalformedHash(t *testing.T) {
	encoded, err := password.Hash("private-test-password")
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(encoded, "$")
	cases := []string{"", "private-test-password", "$argon2id$", encoded + "$extra", strings.Repeat("x", 10000)}
	for i, values := range map[int][]string{
		0: {"x"}, 1: {"argon2i", "bcrypt"}, 2: {"v=0", "v=20"},
		3: {"m=0,t=2,p=1", "m=4294967295,t=2,p=1", "m=19456,t=0,p=1", "m=19456,t=999999999,p=1", "m=19456,t=2,p=0", "m=19456,t=2,p=256", "m=19456,t=2,p=2", "m=019456,t=2,p=1"},
		4: {"!", "AA", parts[4] + "=", parts[4] + "\n"}, 5: {"!", "AA", parts[5] + "=", parts[5] + "\n"},
	} {
		for _, value := range values {
			changed := append([]string(nil), parts...)
			changed[i] = value
			cases = append(cases, strings.Join(changed, "$"))
		}
	}
	for i, bad := range cases {
		if err := password.Verify(bad, "private-test-password"); !errors.Is(err, password.ErrInvalidHash) {
			t.Fatalf("case %d: expected invalid hash", i)
		}
	}
}

func TestEmptyPassword(t *testing.T) {
	if encoded, err := password.Hash(""); encoded != "" || !errors.Is(err, password.ErrInvalidPassword) {
		t.Fatal("empty password accepted")
	}
}

func TestTamperedCredentials(t *testing.T) {
	encoded, err := password.Hash("original-password")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []int{4, 5} {
		parts := strings.Split(encoded, "$")
		data, err := base64.RawStdEncoding.DecodeString(parts[field])
		if err != nil {
			t.Fatal(err)
		}
		data[0] ^= 1
		parts[field] = base64.RawStdEncoding.EncodeToString(data)
		if err := password.Verify(strings.Join(parts, "$"), "original-password"); !errors.Is(err, password.ErrMismatch) {
			t.Fatal("tampered salt or key accepted")
		}
	}
}

func FuzzVerify(f *testing.F) {
	encoded, err := password.Hash("fuzz-password")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add("")
	f.Add("$argon2id$v=19$m=4294967295,t=999999999,p=255$AA$AA")
	f.Fuzz(func(t *testing.T, encoded string) {
		err := password.Verify(encoded, "fuzz-password")
		if err != nil && !errors.Is(err, password.ErrInvalidHash) && !errors.Is(err, password.ErrMismatch) {
			t.Fatal("unexpected error")
		}
	})
}
