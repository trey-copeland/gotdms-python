# Contributing

Collaborators push a branch to this repository (no fork needed) and open a pull request against `main`. `main` is protected by a ruleset: a pull request needs one approving review and these passing checks before it can merge.

- `test (ubuntu, go 1.22)`
- `test (ubuntu, go 1.27)`
- `test (windows, go 1.22)`
- `Test at minimum supported versions (Python 3.10, numpy 1.26, pyarrow 14, polars 2.0)`
- `build wheel and sdist`

The first four are jobs of `.github/workflows/build-and-test-bridge-and-python.yml`; `build wheel and sdist` is `.github/workflows/build-and-verify-python-packages.yml`. To run the same checks locally from the repository root:

```bash
gofmt -l .
go vet ./...
go test -race ./...
python -m unittest discover -s python/tests
```

`gofmt -l` must print nothing. The Python tests build `gotdms-bridge` with `go build` and import the package from `python/src`; they need numpy, Polars, and pyarrow. CI also installs npTDMS 1.13.0 as a test-only oracle that reads files this package wrote. It is not a dependency, and those cross-checks are skipped when it is absent.

Only repository admins can push `v*` tags. A `v*` tag runs `.github/workflows/build-and-publish-python-package.yml`, which publishes to PyPI. Nothing has been published yet; see the README.
