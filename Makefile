export DOCKER_BUILDKIT = 1
export CARGO_BUILD_RUSTFLAGS = -D warnings
export NEAR_RELEASE_BUILD = no
export CARGO_TARGET_DIR = target


# By default, build a regular release
all: release


docker-nearcore: DOCKER_TAG ?= nearcore
docker-nearcore:
	docker build -t $(DOCKER_TAG) -f Dockerfile --build-arg=make_target=neard-release         --progress=plain .

docker-nearcore-sandbox: DOCKER_TAG ?= nearcore-sandbox
docker-nearcore-sandbox:
	docker build -t $(DOCKER_TAG) -f Dockerfile --build-arg=make_target=neard-sandbox-release --progress=plain .

docker-nearcore-nightly: DOCKER_TAG ?= nearcore-nightly
docker-nearcore-nightly:
	docker build -t $(DOCKER_TAG) -f Dockerfile --build-arg=make_target=neard-nightly-release --progress=plain .


release: neard-release
	cargo build -p store-validator --release
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
	cargo build -p store-validator
	cargo build -p genesis-populate
	$(MAKE) sandbox


perf-release: NEAR_RELEASE_BUILD=release
perf-release:
	CARGO_PROFILE_RELEASE_DEBUG=true cargo build -p neard --release --features performance_stats
	cargo build -p store-validator --release --features nearcore/performance_stats


perf-debug:
	cargo build -p neard --features performance_stats
	cargo build -p store-validator --features nearcore/performance_stats


nightly-release: neard-nightly-release
	cargo build -p store-validator --release --features nearcore/nightly,nearcore/performance_stats
	cargo build -p genesis-populate --release --features nearcore/nightly,nearcore/performance_stats

neard-nightly-release:
	cargo build -p neard --release --features nightly,performance_stats


nightly-debug:
	cargo build -p neard --features nightly,performance_stats
	cargo build -p store-validator --features nearcore/nightly,nearcore/performance_stats
	cargo build -p genesis-populate --features nearcore/nightly,nearcore/performance_stats


assertions-release: NEAR_RELEASE_BUILD=release
assertions-release:
	CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true cargo build -p neard --release --features performance_stats

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
.PHONY: perf-release perf-debug nightly-release nightly-debug assertions-release sandbox
.PHONY: sandbox-release
