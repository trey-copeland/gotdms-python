# Contributing

Collaborators push a branch to this repository (no fork needed) and open a pull request against `main`. `main` is protected by a ruleset:

- Changes reach `main` through a pull request.
- A pull request needs one approving review, and the most recent push must be approved. A new push dismisses earlier approvals.
- Force-pushes to `main` and deleting `main` are blocked.
- Repository admins can bypass the ruleset.

These checks must pass before a pull request can merge:

- `test (ubuntu, go 1.22)`
- `test (ubuntu, go 1.27)`
- `test (windows, go 1.22)`
- `Test at minimum supported versions (Python 3.10, numpy 1.26, pyarrow 14, polars 2.0)`
- `build wheel and sdist`

The first four are jobs of `.github/workflows/build-and-test-bridge-and-python.yml`; `build wheel and sdist` is `.github/workflows/build-and-verify-python-packages.yml`. To run the test jobs' main steps locally from the repository root:

```bash
gofmt -l .
go vet ./...
go test -race ./...
python -m unittest discover -s python/tests
```

`gofmt -l` must print nothing. The Python tests build `gotdms-bridge` with `go build` and import the package from `python/src`; they need numpy, Polars, and pyarrow. CI also installs npTDMS 1.13.0 as a test-only oracle that cross-checks files this package writes and reads. It is not a dependency, and those cross-checks are skipped when it is absent.

Only repository admins can create, update, or delete `v*` tags. A `v*` tag runs `.github/workflows/build-and-publish-python-package.yml`. Its publish job needs PyPI trusted publishing and a `pypi` environment. Trusted publishing is not configured, so the job cannot publish yet, and the workflow says not to push a tag before that setup. Nothing has been published; see [`EXTRACTION.md`](EXTRACTION.md).
