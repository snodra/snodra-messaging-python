<p align="center">
  <img alt="Snodra Messaging for Python mascot" src="docs/assets/brand/svg/snodra-messaging.svg" width="160">
</p>

<h1 align="center">Snodra Messaging (Python)</h1>

<p align="center">
  Commands, queries, streams, scatter-gathers and subscriptions for asyncio services, routed by Snodra Server.
</p>

<p align="center">
  <a href="https://github.com/snodra/snodra-messaging-python/actions/workflows/ci.yml"><img alt="ci" src="https://github.com/snodra/snodra-messaging-python/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://pypi.org/project/snodra-messaging/"><img alt="PyPI" src="https://img.shields.io/pypi/v/snodra-messaging"></a>
  <img alt="Python" src="https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white">
  <a href="https://snodra.damoreira.com/docs/messaging/python/"><img alt="docs" src="https://img.shields.io/badge/docs-snodra.damoreira.com-0B5FFF"></a>
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
</p>

---

## What it is

One package, `snodra`, that gives an asyncio service what it needs to send and serve requests in a Snodra system, with the
same wire protocol as services written with any Snodra library: you write async functions, the library keeps the hub streams, permits,
retries and timeouts.

- **Commands and queries** through Snodra Server, with deadlines, retries and routing keys. A command or a query that
  is not shared, of a type the service handles itself, is answered in-process by its own handler, without the hub;
  `Settings(always_route_through_hub=True)` sends it through the hub like any other request.
- **Every query mode**: point, scatter-gather, streaming and subscription; streams and updates are async iterables (`async for`).
- **The hub connection**: registration, heartbeats, reconnects, flow control, several hubs and an ordered shutdown.
- **Credentials and identity**: a token (static, or a file read again), an OAuth2 JWT (renewed) or a client
  certificate (rolled over to a renewed pair); the caller's token verified against the issuer's keys, and guards on
  handlers.
- **Telemetry** behind an interface, with an OpenTelemetry implementation in the separate `snodra-messaging-observability` package.
- **Feature level 1**, proved by the 33 checks of the conformance runner. Events, stores, event processors and sagas
  (levels 2 to 4) are not in it yet.

## Install

```sh
pip install snodra-messaging
pip install snodra-messaging-observability   # optional: OpenTelemetry
```

Both packages are on PyPI and need Python 3.12 or later; the core depends only on `grpcio`, `protobuf` and `cryptography`.

## A handler

```python
@app.command_handler
async def place_order(cmd: PlaceOrder) -> OrderReceipt:
    # A request sent while a request is handled carries its correlation id, the caller's token and the trace on.
    stock = await app.query(CheckStock(items=cmd.items), Stock)
    if not stock.available:
        raise snodra.HandlerError("out of stock", details={"orderId": cmd.order_id})
    return OrderReceipt(order_id=cmd.order_id, total=42)
```

A message is a dataclass with `@snodra.message_type("...")`; its payload is JSON by default (see `snodra.Codec`).
Settings are dataclass options, and `snodra.Settings.from_env()` reads the `SNODRA_*` variables.

## Documentation

Quick start, the settings map, the API and testing: **<https://snodra.damoreira.com/docs/messaging/python/>**. A
complete service is [`examples/quickstart`](examples/quickstart/). The library speaks the Snodra protocol (`snodra.hub.v1`).
Where the specification is silent, [SPEC-GAPS.md](SPEC-GAPS.md) records what this library does.
Counterparts: <https://snodra.damoreira.com/>.

## Contributing

Branches off `main`, conventional commits, a pull request with a green workflow.

## Releases

Versions are cut by CI from the commit subjects since the last tag, in Conventional Commits: a `feat` cuts the next
minor, a `fix` or a `perf` the next patch, and `docs`, `test`, `chore`, `ci`, `refactor`, `build` and `style` cut
nothing. Below 1.0.0 a `!` or a `BREAKING CHANGE:` footer counts as a minor; 1.0.0 is declared by hand. Nobody pushes a
tag: only the workflow makes tags.

Both distributions carry one version and one tag, `vX.Y.Z`. The release commit, `release: X.Y.Z`, writes that version
into both `pyproject.toml` files, makes the observability package require `snodra-messaging==X.Y.Z`, and records them in
`uv.lock` (`scripts/bump-version.sh`).

That commit is pushed only after `scripts/release-check.sh X.Y.Z` has built the two sdists and the two wheels, run
`twine check --strict` on them, installed both wheels on Python 3.12 and 3.14 and run the tests there. It is the check
to run by hand when a release commit is in doubt, and it refuses a version PyPI already has.

Pull before you push: `git pull --rebase` first, because a release leaves a commit by the workflow's bot on `main`.

A failed release is finished, never repeated. **Re-run failed jobs** on the run: the version, the commit and the tag
are already recorded, and the later jobs continue from them; the GitHub release on its own is recreated with the
`release` workflow and the tag. PyPI keeps a version forever, so a wrong release is fixed by the next one, never by
publishing the same version again.

## License

[Apache-2.0](LICENSE) © 2026 Guilherme D'Amoreira.
