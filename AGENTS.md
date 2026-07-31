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

## Version Bump Scope

A version bump changes the `version`, `major`, and `patch` values in `versions.toml`, the files `make generate` writes, and the `CHANGELOG.md` entry for the new tag. Nothing else.

Set `patch` to 0 for the first release of a new `datafusion.version`, then increment it for each later release of that same DataFusion version. `v0.530100.1` and `v0.530100.2` predate this rule; do not read them as precedent.

Documentation that shows a concrete version to teach the tag format is illustrative, not a fact about the current release. Leave it as it is. This includes the comment above `major` in `versions.toml` and the encoding example in the README `Versioning` section: both explain the pattern `v<major>.<encoded-datafusion-version>.<patch>`, and any real version explains it equally well. Do not refresh them with each bump.

Update prose that states the shipped DataFusion version as a fact, and prose that a bump makes wrong.
