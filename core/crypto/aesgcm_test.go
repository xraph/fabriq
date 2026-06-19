package crypto

import (
	"bytes"
	"errors"
	"testing"
)

func testKey() []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i + 1)
	}
	return k
}

func TestAESGCMRoundTrip(t *testing.T) {
	enc, err := NewAESGCM(testKey())
	if err != nil {
		t.Fatalf("NewAESGCM: %v", err)
	}
	aad := []byte("acme")
	ct, err := enc.Encrypt([]byte("s3-secret"), aad)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if bytes.Contains(ct, []byte("s3-secret")) {
		t.Fatal("ciphertext leaks plaintext")
	}
	if ct[0] != 1 {
		t.Fatalf("envelope key-version byte = %d, want 1", ct[0])
	}
	pt, err := enc.Decrypt(ct, aad)
	if err != nil || string(pt) != "s3-secret" {
		t.Fatalf("Decrypt = %q, %v", pt, err)
	}
	// Wrong AAD (different tenant) fails authentication.
	if _, err := enc.Decrypt(ct, []byte("evil")); err == nil {
		t.Fatal("Decrypt with wrong AAD must fail")
	}
	// Unknown key version is rejected.
	bad := append([]byte{9}, ct[1:]...)
	if _, err := enc.Decrypt(bad, aad); !errors.Is(err, ErrKeyVersion) {
		t.Fatalf("bad version err = %v, want ErrKeyVersion", err)
	}
}

func TestNewAESGCMRejectsShortKey(t *testing.T) {
	if _, err := NewAESGCM([]byte("too-short")); err == nil {
		t.Fatal("NewAESGCM must reject a non-32-byte key")
	}
}
