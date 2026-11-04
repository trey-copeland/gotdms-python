module github.com/trey-copeland/gotdms-python

go 1.22.0

// github.com/trey-copeland/gotdms v0.3.0 is commit
// d6481084e2e0a7be1acad332a4f2931b814b1dcb. It reports a negative Dt for
// descending waveforms, which the bridge's range reads match. v0.2.0
// (290bb791b00ad56edeee814960062107d0600cc8) was the first core tag with
// dataset.WithSharedClock and dataset.WithoutTimes, which protocol 2
// reads with. v0.1.2 (8d2f134a160600c8a12b2076cc395c6b05c5c9e2) added the
// metadata-padding skip. This module does not build segment-index
// sidecars. See EXTRACTION.md.
require (
	github.com/apache/arrow-go/v18 v18.0.0
	github.com/trey-copeland/gotdms v0.3.0
)

require (
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/exp v0.0.0-20240909161429-701f63a606c0 // indirect
	golang.org/x/mod v0.21.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/sys v0.26.0 // indirect
	golang.org/x/tools v0.26.0 // indirect
	golang.org/x/xerrors v0.0.0-20231012003039-104605ab7028 // indirect
)
