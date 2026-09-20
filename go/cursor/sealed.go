package cursor

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
)

// Seal frames a confidential envelope with the provider's existing AEAD.
// The provider owns the key lifetime (epoch), associated data and plaintext.
func Seal(aead cipher.AEAD, plain, associated []byte) (string, error) {
	if aead == nil {
		return "", errors.New("cursor: seal unavailable")
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(aead.Seal(nonce, nonce, plain, associated)), nil
}

// Unseal opens the existing nonce+ciphertext envelope. Authorization and
// plaintext interpretation follow at the provider after this succeeds.
func Unseal(aead cipher.AEAD, value string, associated []byte) ([]byte, error) {
	if aead == nil {
		return nil, errors.New("cursor: seal unavailable")
	}
	sealed, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(sealed) < aead.NonceSize()+aead.Overhead() {
		return nil, errors.New("cursor: invalid envelope")
	}
	n := aead.NonceSize()
	return aead.Open(nil, sealed[:n], sealed[n:], associated)
}
