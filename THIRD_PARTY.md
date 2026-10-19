# Third-party notices

This is a package-metadata inventory of the wrapper's runtime stack, not a legal opinion. License texts for code linked into the `gotdms-bridge` binary are copied under `licenses/go/` and listed in `license-files` so they ship in the wheel. Python dependencies below are installed by pip. Their license texts are not copied into this wheel.

## Linked into the gotdms-bridge binary

Versions are the ones selected by `go.mod` when this file was written. `go list -deps` on `./cmd/gotdms-bridge` is the set that is actually linked. `golang.org/x/mod`, `golang.org/x/tools`, and `golang.org/x/sync` appear as indirect module requirements and are not in that link set.

| Module | Version | License | File |
| --- | --- | --- | --- |
| github.com/trey-copeland/gotdms | v0.0.0-20261019014027-1b2458daaea8 (main 1b2458daaea8bf4f424b88fba93228a302ac05c1; not yet tagged) | MIT | licenses/go/gotdms-core-LICENSE |
| github.com/apache/arrow-go/v18 | v18.0.0 | Apache-2.0 | licenses/go/apache-arrow-go-LICENSE.txt |
| github.com/google/flatbuffers | v24.3.25 | Apache-2.0 | licenses/go/flatbuffers-LICENSE |
| github.com/goccy/go-json | v0.10.3 | MIT | licenses/go/goccy-go-json-LICENSE |
| github.com/klauspost/compress | v1.17.11 | BSD-3-Clause | licenses/go/klauspost-compress-LICENSE |
| github.com/klauspost/cpuid/v2 | v2.2.8 | MIT | licenses/go/klauspost-cpuid-LICENSE |
| github.com/pierrec/lz4/v4 | v4.1.21 | BSD-3-Clause | licenses/go/pierrec-lz4-LICENSE |
| github.com/zeebo/xxh3 | v1.0.2 | BSD-2-Clause (xxHash) | licenses/go/zeebo-xxh3-LICENSE |
| golang.org/x/exp | v0.0.0-20240909161429-701f63a606c0 | BSD-3-Clause | licenses/go/golang-x-exp-LICENSE |
| golang.org/x/sys | v0.26.0 | BSD-3-Clause | licenses/go/golang-x-sys-LICENSE |
| golang.org/x/xerrors | v0.0.0-20231012003039-104605ab7028 | BSD-3-Clause | licenses/go/golang-x-xerrors-LICENSE |

The bridge's own code is MIT. See `LICENSE`.

## Installed by pip, not vendored

Recorded from the distributions installed in the packaging environment. Classifiers and license fields are what those projects publish.

| Distribution | Version checked | Requires-Python | License evidence |
| --- | --- | --- | --- |
| numpy | 1.26.4 | >=3.9 | BSD-3-Clause. The package metadata `License` field is the BSD text. https://github.com/numpy/numpy/blob/main/LICENSE.txt |
| polars | 2.0.0 | >=3.10 | MIT. Metadata `License` is the MIT text. https://github.com/pola-rs/polars |
| polars-runtime-32 | 2.0.0 | (pulled by polars) | Classifier `License :: OSI Approved :: MIT License`. This is the Rust runtime wheel Polars depends on. It is not copied into the gotdms wheel. |
| pyarrow | 18.1.0 | >=3.9 | Apache-2.0. Metadata `License` is `Apache Software License`. |

This package depends on `numpy>=1.26`, `polars>=2`, and `pyarrow>=14`, and sets `requires-python` to `>=3.10` because Polars 2.0 does. `scan_tdms` uses `polars.io.plugins.register_io_source` from that release. Polars' pandas extra is not used.
