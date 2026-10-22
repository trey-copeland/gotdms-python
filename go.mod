module github.com/trey-copeland/gotdms-python

go 1.22.0

// github.com/trey-copeland/gotdms has no release tag. This pseudo-version is
// gotdms main at 622cb34bacb80fd810d46ee77183e2bae80bf52d, which includes the
// dataset API, Writer.Stream, and DAQmx raw reads (index words 0x1269,
// 0x1369, and 0x126A). Switch the requirement to v0.1.0 once that tag
// exists. See EXTRACTION.md.
require (
	github.com/apache/arrow-go/v18 v18.0.0
	github.com/trey-copeland/gotdms v0.0.0-20261022022333-622cb34bacb8
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
