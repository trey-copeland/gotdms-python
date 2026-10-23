# gotdms-python

This repository is the Python client and the `gotdms-bridge` binary. It was extracted from `bridge/` in [gotdms](https://github.com/trey-copeland/gotdms) onto the root of [gotdms-python](https://github.com/trey-copeland/gotdms-python). Workflows live at `.github/workflows/`. `pyproject.toml`, `go.mod`, and `python/` are at the root. gotdms removed `bridge/` in [trey-copeland/gotdms#9](https://github.com/trey-copeland/gotdms/pull/9) (9df0060).

## Names

- Repository: `github.com/trey-copeland/gotdms-python`
- Proposed PyPI distribution name: `gotdms` (not published; the name was free on 18 Oct 2026)
- Import package: `gotdms`
- Go module: `github.com/trey-copeland/gotdms-python`

Nothing has been tagged in this repository. `pyproject.toml` records version `0.1.0` for the first release. That is not a git tag. Do not tag, and do not publish to PyPI, until a trusted publisher is a deliberate follow-up. `.github/workflows/build-and-publish-python-package.yml` is that future publisher workflow. It is tag-gated. Trusted publishing is not configured, so it has nothing to run. Point the PyPI trusted publisher at that file when it is set up.

## Core module pin

`go.mod` requires:

```
github.com/trey-copeland/gotdms v0.2.0
```

`v0.2.0` is commit `290bb791b00ad56edeee814960062107d0600cc8`. It is the first core tag with `dataset.WithSharedClock` and `dataset.WithoutTimes`, which protocol 2 reads with. `v0.1.2` is `8d2f134a160600c8a12b2076cc395c6b05c5c9e2`. It skips bytes between the object list and the raw-data offset, so files with that metadata padding, such as NI HSAI DAQmx captures, open. `v0.1.1` is `7d514a7710d8ed18f3884a78c19e4cfcbe2b32a8`. `v0.1.0` is `622cb34bacb80fd810d46ee77183e2bae80bf52d`, which already has the dataset API this binary imports (declared timestamp channels, basis, `ReadMany`), `Writer.Stream`, and DAQmx raw reads (index words `0x1269`, `0x1369`, and `0x126A`). There is no `replace` directive. `go build` and `go test` download this module.

To move the pin:

```bash
go get github.com/trey-copeland/gotdms@v0.2.0
go mod tidy
go test ./...
```

A tag cut from history before the dataset API would not build. Do not point the requirement at one.

## Source distributions and wheels

While this tree lived inside gotdms, an sdist vendored the parent module at `third_party/gotdms`, and a source install rewrote `replace => ../` onto that directory. The committed `go.mod` could not name a version, because no core tag existed and the in-tree replace pointed at `../`.

This repository does not do that.

- `python scripts/build_dist.py` and `python -m build` do not edit `go.mod` and do not vendor gotdms.
- The wheel build runs `go build -trimpath` with `CGO_ENABLED=0`. The Go command resolves gotdms at the version above, and the other modules in `go.sum`, from the module proxy.
- A source install needs Go on `PATH` and network access to the module proxy, the same way it needs them for `github.com/apache/arrow-go/v18`.
- `GOTDMS_PLAT_TAG`, `GOOS`, and `GOARCH` are still how a cross-compile selects the wheel tag. With those unset, the wheel is tagged for the host.

## CI

`.github/workflows/build-and-test-bridge-and-python.yml` runs on push to `main` and on pull requests. Ubuntu tests Go 1.22 and 1.27 (`gofmt`, `go vet`, `go build ./...`, `go test ./...`). Go 1.22 also runs `go test -race ./...` and `python -m unittest discover -s python/tests`. Windows runs `go build ./...`, `go test ./...`, and the Python tests on Go 1.22. A separate job, `Test at minimum supported versions (Python 3.10, numpy 1.26, pyarrow 14, polars 2.0)`, runs the Python tests with Python 3.10, numpy 1.26.0, pyarrow 14.0.0, and polars 2.0.0, the floors in `pyproject.toml`. Every job that installs npTDMS pins `nptdms==1.13.0`. `.github/workflows/build-and-verify-python-packages.yml` builds an sdist and a linux/amd64 wheel on the same events.

## Versions

The core module, this Go module, and the Python package version independently.

- Core tags (`v0.1.0` and later) are the gotdms reader. They are not this package's version. The pin above is the source of truth for which core tag this binary builds against. That pin is `v0.2.0`, which is newer than this package's unreleased `0.1.0`.
- Python `0.1.0` in `pyproject.toml` is this wrapper. Nothing in this repository has been tagged. The first Git tag of this repo can still be `v0.1.0`. It will not match the core tag, because the core module is already at `v0.2.0`. Say so in the changelog when this package is tagged.
- The compatibility contract with a running `gotdms-bridge` is protocol version 2 (`ProtocolVersion` in `frame.go`), not the core tag. A bridge and a client disagree when that number disagrees. Version 1 is not supported by either side.
- The minimum core this binary can compile is `v0.2.0`: protocol 2 reads with `dataset.WithSharedClock`, and values-only reads use `dataset.WithoutTimes`. Neither is in `v0.1.2` or earlier. `hatch_build.py` refuses to package a `go.mod` that replaces gotdms with a local directory. When the pin moves, update it with the commands above, plus the pin text above, `README.md`, `THIRD_PARTY.md`, the `go.mod` comment, and the `hatch_build.py` and `scripts/build_dist.py` docstrings.
