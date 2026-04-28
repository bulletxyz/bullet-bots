# Contributing to bullet-bots

## Getting started

Read [AGENTS.md](AGENTS.md) for the architecture overview, then [HACKING.md](HACKING.md) for a step-by-step guide to adding a strategy.

For adding a new exchange adapter, see [docs/CONTRIBUTING-EXCHANGES.md](docs/CONTRIBUTING-EXCHANGES.md).

## Workflow

1. Fork the repo and create a branch from `master`.
2. Make your changes. Ensure tests pass: `cargo nextest run`.
3. Run `cargo clippy -- -D warnings` and fix any warnings.
4. Run `cargo +nightly fmt` to format.
5. Open a PR against `master` with a clear description of the change.

## Code style

- Formatting is enforced via `rustfmt` (nightly). Config is in `rustfmt.toml`.
- Clippy warnings are treated as errors in CI — fix them, don't suppress them.
- Follow the conventions in [AGENTS.md](AGENTS.md): canonical-source invariant, feed naming, config layout.

## Tests

Every strategy and exchange adapter should have harness-level tests using `ScriptedFeed` and `MockBroker`. See existing tests in `crates/strategies/grid/src/` for examples.

## Issues and discussions

Open a GitHub issue for bugs or feature requests. For questions about the architecture or a contribution you're planning, open an issue first so we can align before you invest time in an implementation.
