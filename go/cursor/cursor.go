// Package cursor owns opaque cursor envelope parsing and epoch/scope binding.
// Providers own payload meaning, retention, authorization and refusal outcomes.
package cursor

import (
	"encoding/hex"
	"strings"
)

// Codec describes an existing provider envelope. The final payload may contain
// separators. A snapshot token can itself supply the epoch; its provider owns
// the bounded snapshot table and checks its scope after lookup.
type Codec struct {
	Separator string
	Scoped    bool
}

type Envelope struct {
	Epoch, Scope, Payload string
}

// Parse extracts the envelope without interpreting the provider payload.
func (c Codec) Parse(value string) (Envelope, bool) {
	if c.Separator == "" {
		return Envelope{}, false
	}
	n := 2
	if c.Scoped {
		n = 3
	}
	parts := strings.SplitN(value, c.Separator, n)
	if len(parts) != n {
		return Envelope{}, false
	}
	e := Envelope{Epoch: parts[0], Payload: parts[n-1]}
	if c.Scoped {
		e.Scope = parts[1]
	}
	return e, true
}

// Encode preserves the selected provider's opaque envelope representation.
func (c Codec) Encode(e Envelope) string {
	if c.Scoped {
		return e.Epoch + c.Separator + e.Scope + c.Separator + e.Payload
	}
	return e.Epoch + c.Separator + e.Payload
}

// Open extracts a payload using a known exact binding. Composite legacy binding
// fields may contain separators; their meaning remains provider-owned. This
// preserves their historical prefix representation without reparsing its parts.
func (c Codec) Open(value string, binding Envelope) (string, bool) {
	binding.Payload = ""
	if c.Separator == "" {
		return "", false
	}
	return strings.CutPrefix(value, c.Encode(binding))
}

// Bound reports exact epoch and scope equality. It grants no authority.
func (e Envelope) Bound(epoch, scope string) bool {
	return e.Epoch == epoch && e.Scope == scope
}

// Hex reports the fixed-width hexadecimal envelope used by stamp observers.
// Payload semantics remain with the observer.
func (e Envelope) Hex(epochBytes, scopeBytes, payloadBytes int) bool {
	for _, field := range []struct {
		value string
		size  int
	}{{e.Epoch, epochBytes}, {e.Scope, scopeBytes}, {e.Payload, payloadBytes}} {
		if len(field.value) != field.size*2 {
			return false
		}
		if _, err := hex.DecodeString(field.value); err != nil {
			return false
		}
	}
	return true
}
