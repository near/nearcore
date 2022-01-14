export CARGO_PROFILE_RELEASE_CODEGEN_UNITS = 1
export CARGO_PROFILE_RELEASE_LTO = fat
export DOCKER_BUILDKIT = 1
export RUSTFLAGS = -D warnings
export NEAR_RELEASE_BUILD = no


# By default, build a regular release
all: release


docker-nearcore:
	docker build -t nearcore         -f Dockerfile --build-arg=make_target=neard-release         --progress=plain .

docker-nearcore-sandbox:
	docker build -t nearcore-sandbox -f Dockerfile --build-arg=make_target=neard-sandbox-release --progress=plain .

docker-nearcore-nightly:
	docker build -t nearcore-nightly -f Dockerfile --build-arg=make_target=neard-nightly-release --progress=plain .


release: neard-release
	cargo build -p state-viewer --release
	cargo build -p store-validator --release
	cargo build -p runtime-params-estimator --release
	cargo build -p genesis-populate --release
	$(MAKE) sandbox-release

neard: neard-release
	@echo 'neard binary ready in ./target/release/neard'

neard-release: NEAR_RELEASE_BUILD=release
neard-release:
	cargo build -p neard --release

neard-debug:
	cargo build -p neard

debug: neard-debug
	cargo build -p near-vm-runner-standalone
	cargo build -p state-viewer
	cargo build -p store-validator
	cargo build -p runtime-params-estimator
	cargo build -p genesis-populate
	$(MAKE) sandbox


perf-release:
	CARGO_PROFILE_RELEASE_DEBUG=true cargo build -p neard --release --features performance_stats,memory_stats
	cargo build -p state-viewer --release --features nearcore/performance_stats,nearcore/memory_stats
	cargo build -p store-validator --release --features nearcore/performance_stats,nearcore/memory_stats


perf-debug:
	cargo build -p neard --features performance_stats,memory_stats
	cargo build -p near-vm-runner-standalone
	cargo build -p state-viewer --features nearcore/performance_stats,nearcore/memory_stats
	cargo build -p store-validator --features nearcore/performance_stats,nearcore/memory_stats


nightly-release: neard-nightly-release
	cargo build -p state-viewer --release --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p store-validator --release --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p runtime-params-estimator --release --features nightly_protocol,nightly_protocol_features,nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p genesis-populate --release --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats

neard-nightly-release:
	cargo build -p neard --release --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats


nightly-debug:
	cargo build -p neard --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo build -p near-vm-runner-standalone --features nightly_protocol,nightly_protocol_features
	cargo build -p state-viewer --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p store-validator --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p runtime-params-estimator --features nightly_protocol,nightly_protocol_features,nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats
	cargo build -p genesis-populate --features nearcore/nightly_protocol,nearcore/nightly_protocol_features,nearcore/performance_stats,nearcore/memory_stats


sandbox: CARGO_TARGET_DIR=sandbox
sandbox: neard-sandbox
	mkdir -p target/debug
	ln -f sandbox/debug/neard target/debug/neard-sandbox
	@ln -f sandbox/debug/neard target/debug/near-sandbox

neard-sandbox:
	cargo build -p neard --features sandbox


sandbox-release: CARGO_TARGET_DIR=sandbox
sandbox-release: neard-sandbox-release
	mkdir -p target/release
	ln -f sandbox/release/neard target/release/neard-sandbox
	@ln -f sandbox/release/neard target/release/near-sandbox

neard-sandbox-release:
	cargo build -p neard --features sandbox --release


.PHONY: docker-nearcore docker-nearcore-nightly release neard debug
.PHONY: perf-release perf-debug nightly-release nightly-debug sandbox
.PHONY: sandbox-release
