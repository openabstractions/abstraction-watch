package cursor

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"testing"
)

func TestEnvelopeBinding(t *testing.T) {
	c := Codec{Separator: ":", Scoped: true}
	want := Envelope{Epoch: "epoch", Scope: "scope", Payload: "position:detail"}
	e, ok := c.Parse(c.Encode(want))
	if !ok || e != want || !e.Bound("epoch", "scope") {
		t.Fatal(e, ok)
	}
	if e.Bound("restart", "scope") || e.Bound("epoch", "foreign") {
		t.Fatal("foreign binding accepted")
	}
	for _, value := range []string{"", "epoch", "epoch:scope"} {
		if _, ok := c.Parse(value); ok {
			t.Fatalf("malformed %q", value)
		}
	}
}

func TestLegacyEnvelopeShapes(t *testing.T) {
	for _, c := range []Codec{{Separator: ":"}, {Separator: "."}, {Separator: ".", Scoped: true}} {
		want := Envelope{Epoch: "snapshot-token", Payload: "0"}
		if c.Scoped {
			want.Scope = "caller"
		}
		got, ok := c.Parse(c.Encode(want))
		if !ok || got != want {
			t.Fatal(c, got)
		}
	}
}

func TestHexEnvelope(t *testing.T) {
	e := Envelope{Epoch: "aA", Scope: "00", Payload: "ff"}
	if !e.Hex(1, 1, 1) {
		t.Fatal(e)
	}
	e.Payload = "fg"
	if e.Hex(1, 1, 1) {
		t.Fatal("nonhex accepted")
	}
	e.Payload = "ffff"
	if e.Hex(1, 1, 1) {
		t.Fatal("wrong length accepted")
	}
}

func TestCompositeBinding(t *testing.T) {
	c := Codec{Separator: ":", Scoped: true}
	b := Envelope{Epoch: "definitions:epoch", Scope: "subject:revision", Payload: "2"}
	value := c.Encode(b)
	if payload, ok := c.Open(value, b); !ok || payload != "2" {
		t.Fatal(payload, ok)
	}
	b.Epoch = "definitions:restart"
	if _, ok := c.Open(value, b); ok {
		t.Fatal("restart accepted")
	}
	b.Epoch, b.Scope = "definitions:epoch", "foreign:revision"
	if _, ok := c.Open(value, b); ok {
		t.Fatal("foreign scope accepted")
	}
}

func TestSealedEnvelopePreservesExistingFraming(t *testing.T) {
	makeAEAD := func(first byte) cipher.AEAD {
		key := make([]byte, 32)
		key[0] = first
		block, err := aes.NewCipher(key)
		if err != nil {
			t.Fatal(err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			t.Fatal(err)
		}
		return aead
	}
	aead := makeAEAD(1)
	plain, scope := []byte("private provider payload"), []byte("profile")
	nonce := make([]byte, aead.NonceSize())
	legacy := base64.RawURLEncoding.EncodeToString(aead.Seal(nonce, nonce, plain, scope))
	got, err := Unseal(aead, legacy, scope)
	if err != nil || string(got) != string(plain) {
		t.Fatal(string(got), err)
	}
	value, err := Seal(aead, plain, scope)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	n := aead.NonceSize()
	got, err = aead.Open(nil, sealed[:n], sealed[n:], scope)
	if err != nil || string(got) != string(plain) {
		t.Fatal(string(got), err)
	}
	for _, value := range []string{"", "malformed!", value[:len(value)/2]} {
		if _, err := Unseal(aead, value, scope); err == nil {
			t.Fatalf("malformed %q", value)
		}
	}
	if _, err := Unseal(makeAEAD(2), value, scope); err == nil {
		t.Fatal("foreign epoch accepted")
	}
	if _, err := Unseal(aead, value, []byte("foreign")); err == nil {
		t.Fatal("foreign scope accepted")
	}
}
