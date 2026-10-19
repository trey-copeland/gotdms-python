# gotdms-python

This repository is the Python client and the `gotdms-bridge` binary. It was extracted from `bridge/` in [gotdms](https://github.com/trey-copeland/gotdms) onto the root of [gotdms-python](https://github.com/trey-copeland/gotdms-python). Workflows live at `.github/workflows/`. `pyproject.toml`, `go.mod`, and `python/` are at the root. `bridge/` is still present in the gotdms repository; removing it there is a separate change.

## Names

- Repository: `github.com/trey-copeland/gotdms-python`
- Proposed PyPI distribution name: `gotdms` (not published; the name was free on 18 Oct 2026)
- Import package: `gotdms`
- Go module: `github.com/trey-copeland/gotdms-python`

Nothing has been tagged in this repository. `pyproject.toml` records version `0.1.0` for the first release. That is not a git tag. Do not tag, and do not publish to PyPI, until a trusted publisher is a deliberate follow-up. `.github/workflows/release.yml` is present and tag-gated. Trusted publishing is not configured, so that workflow has nothing to run.

## Core module pin

gotdms has no release tag. `go.mod` requires:

```
github.com/trey-copeland/gotdms v0.0.0-20261019014027-1b2458daaea8
```

That pseudo-version is commit `1b2458daaea8bf4f424b88fba93228a302ac05c1` on gotdms `main`. It includes the dataset API this binary imports (declared timestamp channels, basis, `ReadMany`). There is no `replace` directive. `go build` and `go test` download this module.

When gotdms is tagged `v0.1.0` from that commit or a later one that still contains the dataset API, switch the requirement:

```bash
go get github.com/trey-copeland/gotdms@v0.1.0
go mod tidy
go test ./...
```

A tag cut from history before the dataset API would not build. Do not point the requirement at one.

## Source distributions and wheels

While this tree lived inside gotdms, an sdist vendored the parent module at `third_party/gotdms`, and a source install rewrote `replace => ../` onto that directory. The committed `go.mod` could not name a version, because no core tag existed and the in-tree replace pointed at `../`.

This repository does not do that.

- `python scripts/build_dist.py` and `python -m build` do not edit `go.mod` and do not vendor gotdms.
- The wheel build runs `go build -trimpath` with `CGO_ENABLED=0`. The Go command resolves gotdms at the pseudo-version above, and the other modules in `go.sum`, from the module proxy.
- A source install needs Go on `PATH` and network access to the module proxy, the same way it needs them for `github.com/apache/arrow-go/v18`.
- `GOTDMS_PLAT_TAG`, `GOOS`, and `GOARCH` are still how a cross-compile selects the wheel tag. With those unset, the wheel is tagged for the host.

## CI

`.github/workflows/ci.yml` runs on push to `main` and on pull requests. Ubuntu tests Go 1.22 and 1.27 (`gofmt`, `go vet`, `go build ./...`, `go test ./...`). Go 1.22 also runs `go test -race ./...` and `python -m unittest discover -s python/tests`. Windows runs `go build ./...` and `go test ./...` on Go 1.22. `.github/workflows/wheels.yml` builds an sdist and a linux/amd64 wheel on the same events.

## Versions

The core module, this Go module, and the Python package version independently.

- Core tags (`v0.1.0` and later) are the gotdms reader. They are not this package's version. Until the first core tag, the pin above is the source of truth.
- Python `0.1.0` in `pyproject.toml` is this wrapper. The first Git tag of this repo should be `v0.1.0` so the Go module version and the Python version match for that first release. Later tags may diverge. Say so in the changelog when they do.
- The compatibility contract with a running `gotdms-bridge` is protocol version 1 (`ProtocolVersion` in `frame.go`), not the core tag. A bridge and a client disagree when that number disagrees.
- The minimum core source this binary can compile is the dataset API. That API is the commit pinned in `go.mod`. No released core tag provides it yet.
