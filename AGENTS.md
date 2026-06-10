# AGENTS.md

## Verification Requirement

Before handing off code changes, lint and test all Go and Rust code in this repository. Run the project lint target and Go/Rust test targets, and report any command that cannot be run.

Required baseline:

```sh
make lint
make test
make test.source
make rust.test
```

For release-sensitive changes, prefer the full release verification targets documented in `CONTRIBUTING.md`.

## Version Source of Truth

`versions.toml` is the only human-maintained release/version source. Do not hand-edit generated version constants in `version.go`, `internal/native/version_generated.go`, `rust/src/generated.rs`, or generated version/dependency fields in `rust/Cargo.toml`.

After changing `versions.toml`, run:

```sh
make generate
make generate.check
```

Commit the generated Go/Rust files and `rust/Cargo.lock` updates with the `versions.toml` change.
