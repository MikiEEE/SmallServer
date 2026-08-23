# Development

## Set up

Use Python 3.10 or newer and install the pinned SmallOS v1.2.0 release plus
SmallServer in editable mode:

```console
python3 -m pip install -r requirements.txt
python3 -m pip install -e '.[test]'
```

For reproducible validation, put the canonical SmallOS checkout at the front of
`PYTHONPATH` rather than relying on an unrelated installed package named
`SmallPackage`.

## Validate

```console
python3 -m unittest discover -s tests -v
python3 -m compileall -q smallserver demo.py examples tests
git diff --check
```

The suite covers routing, HTTP values and parsing, adapters, lifecycle failure
ownership, kernel transport behavior, and real loopback serving when the local
environment permits binds. Documentation tests verify the tracked guide set,
relative Markdown links, and Python code-block syntax.

In a separate clean environment, verify the lazy optional-dependency boundary
without installing the test or HTTP/2 extras:

```console
python3 -m pip install -r requirements.txt
python3 -m pip install -e .
python3 -m unittest tests.test_http2 -v
python3 -m unittest tests.test_regex_routing -v
python3 -m unittest tests.test_websocket -v
```

The dependency-contract tests run and optional interoperability cases skip
cleanly; importing and testing ordinary HTTP/1.1 must require neither
hyper-h2, regex, nor wsproto.

Run the examples when their platform requirements are available:

```console
python3 demo.py
python3 examples/adapters_demo.py
python3 examples/manual_runtime.py
python3 examples/websocket_echo.py
```

The two network examples block until shutdown. `adapters_demo.py` completes on
its own and demonstrates SQLite thread affinity and a persistent asyncio loop.

## Contribution boundaries

- Keep framework networking behind SmallOS kernel abstractions.
- Preserve finite parsing, connection, and adapter limits.
- Keep the HTTP core independent of `asyncio`.
- Add lifecycle tests for partial acquisition and cleanup failure paths.
- Update the README and focused guide page when a public API changes.
- Extend [Protocol roadmap](protocol-roadmap.md) docs on the feature branch that
  implements a protocol; do not describe planned APIs as present.

The ignored `docs/` and `skills/` trees support local agent workflows. Public,
versioned user documentation belongs in `README.md` and `guide/`.
