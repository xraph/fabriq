package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// keyVersion1 tags v1 ciphertext. A future keyring maps version bytes to keys;
// the envelope format never changes, so rotation is a background re-encrypt.
const keyVersion1 byte = 1

// AESGCM implements Encryptor with AES-256-GCM. Envelope layout:
//
//	[1-byte keyVersion][12-byte nonce][ciphertext+tag]
type AESGCM struct {
	aead cipher.AEAD
}

// NewAESGCM builds an AES-256-GCM encryptor from a 32-byte key.
func NewAESGCM(key []byte) (*AESGCM, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("fabriq: crypto: key must be 32 bytes (AES-256), got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &AESGCM{aead: aead}, nil
}

// Encrypt seals plaintext with a fresh random nonce, binding aad. The returned
// envelope is version || nonce || ciphertext+tag.
func (a *AESGCM) Encrypt(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, a.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	out := make([]byte, 0, 1+len(nonce)+len(plaintext)+a.aead.Overhead())
	out = append(out, keyVersion1)
	out = append(out, nonce...)
	return a.aead.Seal(out, nonce, plaintext, aad), nil
}

// Decrypt opens an envelope produced by Encrypt, verifying aad.
func (a *AESGCM) Decrypt(ciphertext, aad []byte) ([]byte, error) {
	ns := a.aead.NonceSize()
	if len(ciphertext) < 1+ns {
		return nil, fmt.Errorf("fabriq: crypto: ciphertext too short")
	}
	if ciphertext[0] != keyVersion1 {
		return nil, ErrKeyVersion
	}
	nonce := ciphertext[1 : 1+ns]
	return a.aead.Open(nil, nonce, ciphertext[1+ns:], aad)
}
