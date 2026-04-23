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

# Run a single benchmark with perf counters (software events; works in VMs)
bench-perf name:
    perf stat -e task-clock,page-faults,major-faults,minor-faults,context-switches,cpu-migrations,alignment-faults,duration_time,user_time,system_time \
        cargo bench -- "{{name}}"

# Compare wall-clock vs CPU time to spot I/O stalls (user+sys << wall = I/O-bound)
bench-perf-io name:
    perf stat -e duration_time,user_time,system_time,task-clock,major-faults,minor-faults \
        cargo bench -- "{{name}}"

# Profile and generate a flamegraph (requires `cargo install flamegraph`)
bench-flamegraph name:
    cargo flamegraph --bench bench_insert -- --bench --profile-time 5 "{{name}}"
