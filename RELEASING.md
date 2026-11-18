# Releasing chargewire-adbc

A release is two tags on one commit of `main`: `vX.Y.Z`, which `release.yml` acts on (the full
test gate, the shared library for every platform, the wheels, the GitHub Release and PyPI
`adbc-driver-chargewire`), and `go/vX.Y.Z`, which versions the Go module
`github.com/gizmodata/chargewire-adbc/go/v2` (it lives in `go/`, so Go's subdirectory-module convention
applies; `go-module.yml` checks the pair and warms the module proxy). The Python package and the
Go module share one version line. From 2.0.0 the module path ends in `/v2`, as Go's semantic import
versioning requires.

## Order

1. Merge the release pull request through branch protection with green required checks:
   the version in `python/pyproject.toml` and `_version.py`, and `CHANGELOG.md`'s
   `## [X.Y.Z] - YYYY-MM-DD` section with today's date. Before merging, run `release.yml` on the
   release branch (`gh workflow run release.yml --ref <branch>`) and wait for every job to pass:
   a dispatch runs everything a tag runs except the GitHub Release and PyPI, so a failure costs
   no tag. Fetch `origin/main` and verify it is the exact landed commit and has the tested tree
   (`git rev-parse origin/main^{tree}` equals the dispatched commit's).
2. Tag that landed commit and push only the release tag first:

   ```bash
   git fetch origin main
   sha=$(git rev-parse origin/main)
   git tag -a vX.Y.Z -m "chargewire-adbc X.Y.Z" "$sha"
   git push origin vX.Y.Z
   ```

   Wait for `release.yml` to pass and verify the published release before creating the
   Go tag on the same commit. Go tags can be recorded permanently by the checksum database;
   never move either tag.

   ```bash
   sha=$(git rev-parse 'vX.Y.Z^{commit}')
   git tag -a go/vX.Y.Z -m "github.com/gizmodata/chargewire-adbc/go/v2 vX.Y.Z" "$sha"
   git push origin go/vX.Y.Z
   ```

   `release.yml` runs on `vX.Y.Z` (its `v*` trigger does not match `go/v*`); `go-module.yml` runs on
   `go/vX.Y.Z` and refuses a commit that `vX.Y.Z` is not on.
3. Wait for `go-module.yml`, then check what was published:

   ```bash
   curl -sS https://pypi.org/pypi/adbc-driver-chargewire/json | python3 -c 'import json,sys; print(json.load(sys.stdin)["info"]["version"])'
   gh release view vX.Y.Z -R gizmodata/chargewire-adbc --json assets --jq '.assets[].name'
   GOFLAGS=-mod=mod GOPROXY=https://proxy.golang.org go list -m github.com/gizmodata/chargewire-adbc/go/v2@vX.Y.Z
   ```

   The release has 12 assets: six library archives, five wheels and `SHA256SUMS` (the
   installer at [gizmodata/adbc-install](https://github.com/gizmodata/adbc-install) checks an
   archive against it before installing). Download them all and verify the sums:

   ```bash
   d=$(mktemp -d); gh release download vX.Y.Z -R gizmodata/chargewire-adbc -D "$d"
   (cd "$d" && sha256sum -c SHA256SUMS && test "$(wc -l < SHA256SUMS)" -eq 11)
   ```

   The `go list` line is what downstream Go modules depend on: `hardcharger-adbc` moves its
   `go/go.mod` from a pseudo-version to `github.com/gizmodata/chargewire-adbc/go/v2 vX.Y.Z` only
   once it prints the version (see that repository's `RELEASING.md`).

## While the repository is internal

`proxy.golang.org` cannot read an internal repository, so a `go/vX.Y.Z` tag can only be served
once the repository is public: push the Go tag after the visibility flip (the `warm` job fails
before that and can be re-run). Downstream modules depend on a pseudo-version
(`v0.0.0-<date>-<sha>`) fetched over SSH with a deploy key and `GOPRIVATE=github.com/gizmodata/*`
until then; that plumbing goes away with the flip.
