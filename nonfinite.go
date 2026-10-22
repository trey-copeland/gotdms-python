package bridge

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"math"
)

// JSON has no NaN or infinity, and encoding/json refuses them. TDMS float
// properties can hold them, so a header value goes through jsonFloat: a
// non-finite value is marshaled as a placeholder string, and writeResult
// swaps each placeholder for the bare NaN, Infinity, or -Infinity token.
// Python's json module reads those tokens as float('nan') and float('inf').
//
// The placeholder holds a per-process random tag, so a string property
// cannot match it by accident.
var nonFiniteTag = func() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return "\x00gotdms-nonfinite-" + hex.EncodeToString(b[:]) + "-"
}()

var nonFiniteTokens = []string{"NaN", "Infinity", "-Infinity"}

type nonFinite float64

func (v nonFinite) MarshalJSON() ([]byte, error) {
	return json.Marshal(nonFiniteTag + nonFiniteName(float64(v)))
}

func nonFiniteName(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	default:
		return "NaN"
	}
}

// jsonFloat returns f, or a nonFinite marker when f is NaN or infinite.
func jsonFloat(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nonFinite(f)
	}
	return f
}

// restoreNonFinite replaces each nonFinite placeholder in b with its token.
func restoreNonFinite(b []byte) []byte {
	tagJSON, _ := json.Marshal(nonFiniteTag)
	prefix := tagJSON[:len(tagJSON)-1] // no closing quote
	if !bytes.Contains(b, prefix) {
		return b
	}
	for _, tok := range nonFiniteTokens {
		quoted, _ := json.Marshal(nonFiniteTag + tok)
		b = bytes.ReplaceAll(b, quoted, []byte(tok))
	}
	return b
}
