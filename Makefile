export CARGO_PROFILE_RELEASE_CODEGEN_UNITS = 1
export CARGO_PROFILE_RELEASE_LTO = fat
export DOCKER_BUILDKIT = 1
export RUSTFLAGS = -D warnings


# By default, build a regular release
all: release


# TODO All three of these commands - docker-nearcore, docker-nearcore-sandbox, and docker-nearcore-nightly - should use the same Dockerfile, with a build ARG
docker-nearcore:
	docker build -t nearcore -f Dockerfile --progress=plain .

docker-nearcore-sandbox:
	docker build -t nearcore-sandbox -f Dockerfile.sandbox --progress=plain .

docker-nearcore-nightly:
	docker build -t nearcore-nightly -f Dockerfile.nightly --progress=plain .

release:
	cargo build -p neard --release
	cargo build -p state-viewer --release
	cargo build -p store-validator --release
	cargo build -p runtime-params-estimator --release
	cargo build -p genesis-populate --release
	make sandbox-release

neard:
	cargo build -p neard --release --bin neard
	@echo 'neard binary ready in ./target/release/neard'

debug:
	cargo build -p neard
	cargo build -p near-vm-runner-standalone
	cargo build -p state-viewer
	cargo build -p store-validator
	cargo build -p runtime-params-estimator
	cargo build -p genesis-populate
	make sandbox

perf-release:
	CARGO_PROFILE_RELEASE_DEBUG=true cargo build -p neard --release --features performance_stats,memory_stats
	cargo build -p state-viewer --release --features nearcore/performance_stats,nearcore/memory_stats
	cargo build -p store-validator --release --features nearcore/performance_stats,nearcore/memory_stats

perf-debug:
	cargo build -p neard --features performance_stats,memory_stats
	cargo build -p near-vm-runner-standalone
	cargo build -p state-viewer --features nearcore/performance_stats,nearcore/memory_stats
	cargo build -p store-validator --features nearcore/performance_stats,nearcore/memory_stats

nightly-release:
	cargo build -p neard --release --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo build -p state-viewer --release --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p store-validator --release --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p runtime-params-estimator --release --features nightly_protocol,nightly_protocol_features,nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p genesis-populate --release --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats

nightly-debug:
	cargo build -p neard --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo build -p near-vm-runner-standalone --features nightly_protocol,nightly_protocol_features
	cargo build -p state-viewer --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p store-validator --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p runtime-params-estimator --features nightly_protocol,nightly_protocol_features,nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p genesis-populate --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats

sandbox:
	CARGO_TARGET_DIR=sandbox cargo build -p neard --features sandbox
	mkdir -p target/debug
	mv sandbox/debug/neard target/debug/near-sandbox

sandbox-release:
	CARGO_TARGET_DIR=sandbox cargo build -p neard --features sandbox --release
	mkdir -p target/release
	mv sandbox/release/neard target/release/near-sandbox


.PHONY: docker-nearcore docker-nearcore-nightly release neard debug
.PHONY: perf-release perf-debug nightly-release nightly-debug sandbox
.PHONY: sandbox-release
