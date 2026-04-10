# Contributing to DebeTail

Contributions are welcome and appreciated — whether that's fixing a bug, improving docs,
or adding test coverage.

## Before you start

To avoid wasted effort, please **open an issue first** for anything beyond a small bug fix or
doc change. This lets us discuss scope and confirm the change fits the roadmap before you
invest time writing code.

## About this project's licence

DebeTail is a dual-licensed project — the free tier (this repo) is MIT; the commercial tier
is BSL. Like many dual-licensed projects, we ask contributors to sign a brief
**Contributor Licence Agreement (CLA)** before merging. This lets KloudShift distribute your
contribution under either licence. The CLA bot will prompt you automatically on your first PR.

## Getting started

```bash
# Requirements: .NET 10.0 SDK
dotnet build DebeTail-Free/DebeTail.slnx
dotnet test  DebeTail-Free/DebeTail.slnx
```

Integration tests for the Postgres connector require a live Postgres instance with logical
replication enabled — see `src/DebeTail.Sources.Postgres/README.md`.

## What makes a good PR

- Fixes a confirmed bug or addresses an open issue
- Includes tests for the changed behaviour
- Passes `dotnet build` with zero warnings (`TreatWarningsAsErrors` is on)
- Keeps public interfaces in `DebeTail.Abstractions` stable — these are the locked contract
  shared across all packages

## Questions and ideas

Use [GitHub Discussions](../../discussions) for questions, ideas, or feedback.
Issues are for confirmed bugs and accepted feature requests.