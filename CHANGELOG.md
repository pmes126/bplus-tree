# Changelog

All notable changes to this project will be documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-1.0: minor versions may include breaking changes. See `ROADMAP.md` for the
path to a stable 1.0.0.

## [Unreleased]
### Added
- Isolated `loom-model` crate: a loom model of the commit publish protocol,
  exercising all concurrent-writer interleavings (currently on branch
  `CASPtrPlusGeneration`, not yet in `main`).
- `ROADMAP.md` documenting known limitations and the path to 1.0.

## [0.7.0] - 2026-07-04
### Changed
- Commit path now publishes `(metadata pointer, height, txn_id)` via a single
  128-bit atomic CAS, eliminating the ABA/UAF hazards of the previous scheme
  without needing `ArcSwap` alongside the `EpochManager` (#29).
### Fixed
- Durability corrections and associated test fixes (#27, #28).

## [0.6.0] - 2026-05-31
### Removed
- Unused prefix key-format implementation and references (#26).

## [0.5.0] - 2026-05-22
### Added
- Per-tree page reclamation: all pages of a tree are reclaimed when the tree is
  dropped (#22).
### Changed
- Ongoing storage/concurrency hardening (epoch-based reclamation, metadata A/B
  slots, manifest-log catalog durability).

## [0.1.0] - 2025-06-24
### Added
- Initial implementation: copy-on-write B+-tree core, storage/page/codec layers,
  multi-tree catalog, and basic operations with tests.
