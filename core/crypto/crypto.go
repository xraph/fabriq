// Package crypto provides field-level encryption for fabriq (e.g. blob_source
// credentials). It is core-pure: stdlib only, zero TwinOS knowledge.
package crypto

import "errors"

// Encryptor encrypts and decrypts opaque byte fields. aad (additional
// authenticated data) binds the ciphertext to its context (e.g. the tenant id)
// so a stolen ciphertext cannot be replayed into another row; the SAME aad must
// be supplied to Decrypt.
type Encryptor interface {
	Encrypt(plaintext, aad []byte) ([]byte, error)
	Decrypt(ciphertext, aad []byte) ([]byte, error)
}

var (
	// ErrNotConfigured is returned by callers when encryption is required but no
	// key was configured.
	ErrNotConfigured = errors.New("fabriq: encryption not configured")
	// ErrKeyVersion is returned when a ciphertext's key-version byte is unknown.
	ErrKeyVersion = errors.New("fabriq: ciphertext key version not recognized")
)
