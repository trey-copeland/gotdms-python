package bridge

import (
	"math"
	"testing"

	"github.com/trey-copeland/gotdms/dataset"
)

// channelYs decodes each channel stream's y column, in header order.
func channelYs(t *testing.T, r reply) [][]float64 {
	t.Helper()
	if r.Op != "result" || r.Data == nil {
		t.Fatalf("reply %+v", r)
	}
	streams, _, err := r.Data.legacy()
	if err != nil {
		t.Fatal(err)
	}
	out := make([][]float64, len(streams))
	for i, b := range streams {
		cols, _, err := decodeIPC(b)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = cols["y"]
	}
	return out
}

// A read with neither absolute nor relative time reads WithoutTimes: the
// dataset series carry no Times and no Clock, and y is what a timed read
// returns.
func TestValuesOnlyReadBuildsNoTimes(t *testing.T) {
	paths := writeV2Set(t, t.TempDir(), 40)
	c := start(t)
	defer c.close()
	openV2(t, c, paths)
	var seen []dataset.Series
	seriesHook = func(s []dataset.Series) { seen = append([]dataset.Series(nil), s...) }
	defer func() { seriesHook = nil }()

	check := func(what string, req map[string]any, wantTimes bool) [][]float64 {
		t.Helper()
		seen = nil
		r := c.call(t, req)
		if len(seen) == 0 {
			t.Fatalf("%s: hook not called", what)
		}
		for _, s := range seen {
			has := s.Times != nil || s.Clock != nil
			if s.Timed && has != wantTimes {
				t.Fatalf("%s: %s/%s times=%d clock=%v, want times %v", what, s.Group, s.Name, len(s.Times), s.Clock != nil, wantTimes)
			}
		}
		return channelYs(t, r)
	}
	for _, w := range []map[string]any{
		{},
		{"start": "2024-03-01T00:00:01Z", "end": "2024-03-01T00:00:03Z"},
	} {
		base := map[string]any{"op": "read_many", "channels": v2Sels[:5]}
		for k, v := range w {
			base[k] = v
		}
		with := func(abs, rel bool) map[string]any {
			m := map[string]any{"absolute": abs, "relative": rel}
			for k, v := range base {
				m[k] = v
			}
			return m
		}
		timed := check("absolute", with(true, false), true)
		rel := check("relative", with(false, true), true)
		vals := check("values-only", with(false, false), false)
		for i := range timed {
			if len(timed[i]) == 0 {
				t.Fatalf("channel %d: empty read", i)
			}
			for _, got := range [][]float64{rel[i], vals[i]} {
				if len(got) != len(timed[i]) {
					t.Fatalf("channel %d: len %d vs %d", i, len(got), len(timed[i]))
				}
				for k := range got {
					if math.Float64bits(got[k]) != math.Float64bits(timed[i][k]) {
						t.Fatalf("channel %d sample %d: %v vs %v", i, k, got[k], timed[i][k])
					}
				}
			}
		}
	}
	// Offset/limit reads build times to order merged samples; a values-only
	// reply drops them before encoding and still matches.
	lim := check("limited", map[string]any{"op": "read_many", "channels": v2Sels[:2], "offset": 3, "limit": 20, "absolute": false, "relative": false}, true)
	full := check("limited timed", map[string]any{"op": "read_many", "channels": v2Sels[:2], "offset": 3, "limit": 20, "relative": false}, true)
	for i := range lim {
		if len(lim[i]) != 20 || len(full[i]) != 20 || lim[i][0] != full[i][0] || lim[i][19] != full[i][19] {
			t.Fatalf("limited channel %d: %v vs %v", i, lim[i], full[i])
		}
	}
}
