# Releasing SmallServer

SmallServer cuts a GitHub release after CI succeeds for the exact commit merged
into `main`. The release contains the source distribution and wheel built from
that commit, a generated changelog, and GitHub artifact provenance.

## Release flow

1. Merge feature pull requests into `develop` and keep CI green.
2. Prepare a release pull request from `develop` to `main`.
3. Update `project.version` in `pyproject.toml` to a version that does not
   already have a `v<version>` tag.
4. Review user documentation and release-facing metadata in that pull request.
5. Merge it into `main`.
6. The `CI` workflow installs the complete test extra, exercises the core,
   regex-routing, WebSocket, and HTTP/2 suites on Python 3.10 and 3.12, compiles
   the source, builds the wheel and source archive, and checks both
   distributions.
7. Only after that exact `main` commit succeeds, the `Release` workflow verifies
   it is still the tip of `main`, requires a new version, rebuilds and attests
   the distributions, creates the `v<version>` tag, and creates the GitHub
   release.

If another commit reaches `main` first, the stale workflow exits without
releasing; the newer commit's CI run owns the release. If the version tag
already exists, release creation fails visibly and the next release pull
request must bump `project.version`.

## Local release checks

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -e '.[test]'
python3 -m unittest discover -s tests -v
python3 -m compileall -q smallserver tests examples
python3 -m pip install --upgrade build twine
python3 -m build
python3 -m twine check dist/*
```

## Publishing boundary

The automated process creates a GitHub release; it does not publish to PyPI.
SmallServer currently installs SmallOS from its canonical Git `master` branch
through `requirements.txt`, while `pyproject.toml` intentionally has no runtime
dependency declaration. Publishing the wheel to PyPI before SmallOS has an
installable release dependency would give users an incomplete installation.

Add PyPI trusted publishing only after SmallOS has a stable package release,
SmallServer declares that dependency in `pyproject.toml`, and an installed-wheel
test proves a clean environment receives every runtime dependency.
