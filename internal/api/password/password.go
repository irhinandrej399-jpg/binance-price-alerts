// Package password hashes API credentials. Passwords and encoded hashes are
// secrets: callers must not log them or include them in public responses.
package password

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strings"

	"golang.org/x/crypto/argon2"
)

var (
	ErrInvalidPassword = errors.New("password must not be empty")
	ErrInvalidHash     = errors.New("invalid or unsupported password hash")
	ErrMismatch        = errors.New("password does not match")
)

// Only this profile is supported. Never use untrusted cost parameters directly:
// a corrupted database value must not control memory allocations or CPU work.
const (
	memory   = 19 * 1024 // KiB (19 MiB)
	passes   = 2
	threads  = 1
	saltSize = 16
	keySize  = 32
	prefix   = "$argon2id$v=19$m=19456,t=2,p=1$"
)

// Hash returns a PHC-format Argon2id v19 hash with a fresh random 128-bit salt.
// It preserves the password bytes, including whitespace, Unicode and NULs.
// Password strength and request size policies belong to the calling API.
func Hash(plain string) (string, error) {
	if plain == "" {
		return "", ErrInvalidPassword
	}
	salt := make([]byte, saltSize)
	// crypto/rand.Read fills the buffer or terminates the process on RNG failure.
	rand.Read(salt)
	key := argon2.IDKey([]byte(plain), salt, passes, memory, threads, keySize)
	return prefix + base64.RawStdEncoding.EncodeToString(salt) + "$" + base64.RawStdEncoding.EncodeToString(key), nil
}

// Verify returns nil on a match, ErrMismatch on an incorrect password, and
// ErrInvalidHash for malformed or unsupported stored values. No error contains
// credentials. A future profile change must explicitly preserve old profiles.
func Verify(encoded, plain string) error {
	const saltEncodedSize = (saltSize*8 + 5) / 6
	const keyEncodedSize = (keySize*8 + 5) / 6
	if len(encoded) != len(prefix)+saltEncodedSize+1+keyEncodedSize || !strings.HasPrefix(encoded, prefix) {
		return ErrInvalidHash
	}
	body := encoded[len(prefix):]
	if body[saltEncodedSize] != '$' {
		return ErrInvalidHash
	}
	salt, ok := decode(body[:saltEncodedSize], saltSize)
	if !ok {
		return ErrInvalidHash
	}
	expected, ok := decode(body[saltEncodedSize+1:], keySize)
	if !ok {
		return ErrInvalidHash
	}
	if plain == "" {
		return ErrMismatch
	}
	actual := argon2.IDKey([]byte(plain), salt, passes, memory, threads, keySize)
	if subtle.ConstantTimeCompare(actual, expected) != 1 {
		return ErrMismatch
	}
	return nil
}

func decode(encoded string, size int) ([]byte, bool) {
	decoded, err := base64.RawStdEncoding.Strict().DecodeString(encoded)
	// Go's decoder ignores CR/LF; re-encoding also enforces canonical input.
	if err != nil || len(decoded) != size || base64.RawStdEncoding.EncodeToString(decoded) != encoded {
		return nil, false
	}
	return decoded, true
}
