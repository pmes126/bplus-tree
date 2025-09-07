default: clippy test

fmt:
    cargo fmt --all

clippy:
    cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --all-features --workspace

doc:
    cargo doc --no-deps --all-features --open

bench:
    cargo bench
