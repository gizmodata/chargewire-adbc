# Releasing warp-sql-adbc

A release is two tags on one commit of `main`: `vX.Y.Z`, which `release.yml` acts on (the full
test gate, the shared library for every platform, the wheels, the GitHub Release and PyPI
`adbc-driver-warpsql`), and `go/vX.Y.Z`, which versions the Go module
`github.com/gizmodata/warp-sql-adbc/go` (it lives in `go/`, so Go's subdirectory-module convention
applies; `go-module.yml` checks the pair and warms the module proxy). The Python package and the
Go module share one version line.

## Order

1. On `main`, green CI: the version in `python/pyproject.toml` and `_version.py`, and
   `CHANGELOG.md`'s `## [X.Y.Z] - YYYY-MM-DD` section with today's date.
2. Tag the release, then the Go module, on that commit, and push the release tag first:

   ```bash
   sha=$(git rev-parse main)
   git tag -a vX.Y.Z -m "warp-sql-adbc X.Y.Z" "$sha"
   git tag -a go/vX.Y.Z -m "github.com/gizmodata/warp-sql-adbc/go vX.Y.Z" "$sha"
   git push origin vX.Y.Z
   git push origin go/vX.Y.Z
   ```

   `release.yml` runs on `vX.Y.Z` (its `v*` trigger does not match `go/v*`); `go-module.yml` runs on
   `go/vX.Y.Z` and refuses a commit that `vX.Y.Z` is not on.
3. Wait for both workflows, then check what was published:

   ```bash
   curl -sS https://pypi.org/pypi/adbc-driver-warpsql/json | python3 -c 'import json,sys; print(json.load(sys.stdin)["info"]["version"])'
   gh release view vX.Y.Z -R gizmodata/warp-sql-adbc --json assets --jq '.assets[].name'
   GOFLAGS=-mod=mod GOPROXY=https://proxy.golang.org go list -m github.com/gizmodata/warp-sql-adbc/go@vX.Y.Z
   ```

   The last line is what downstream Go modules depend on: `hardcharger-adbc` moves its `go/go.mod`
   from a pseudo-version to `github.com/gizmodata/warp-sql-adbc/go vX.Y.Z` only once it prints
   the version (see that repository's `RELEASING.md`).

## While the repository is internal

`proxy.golang.org` cannot read an internal repository, so a `go/vX.Y.Z` tag can only be served
once the repository is public: push the Go tag after the visibility flip (the `warm` job fails
before that and can be re-run). Downstream modules depend on a pseudo-version
(`v0.0.0-<date>-<sha>`) fetched over SSH with a deploy key and `GOPRIVATE=github.com/gizmodata/*`
until then; that plumbing goes away with the flip.
