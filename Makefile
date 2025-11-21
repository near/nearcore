#cspell:words BUILDKIT

export DOCKER_BUILDKIT = 1
export CARGO_BUILD_RUSTFLAGS = -D warnings
export NEAR_RELEASE_BUILD = no
export OPENSSL_STATIC = 1
export CARGO_TARGET_DIR = target


# By default, build a regular release
#? all: build a regular release of neard
all: release

#? docker-nearcore: build nearcore docker image with tag 'nearcore'
docker-nearcore: DOCKER_TAG ?= nearcore
docker-nearcore:
	docker build -t $(DOCKER_TAG) -f Dockerfile --build-arg=make_target=neard-release         --progress=plain .

#? docker-nearcore-sandbox: build nearcore docker image with tag 'nearcore-sandbox'
docker-nearcore-sandbox: DOCKER_TAG ?= nearcore-sandbox
docker-nearcore-sandbox:
	docker build -t $(DOCKER_TAG) -f Dockerfile --build-arg=make_target=neard-sandbox-release --progress=plain .

#? docker-nearcore-nightly: build nearcore docker image with tag 'nearcore-nightly'
docker-nearcore-nightly: DOCKER_TAG ?= nearcore-nightly
docker-nearcore-nightly:
	docker build -t $(DOCKER_TAG) -f Dockerfile --build-arg=make_target=neard-nightly-release --progress=plain .


release: neard-release
	$(MAKE) sandbox-release

#? neard: build release version of neard and echo built binary path
neard: neard-release
	@echo 'neard binary ready in ./target/release/neard'

neard-release: NEAR_RELEASE_BUILD=release
neard-release:
	cargo build -p neard --release

neard-debug:
	cargo build -p neard

#? debug: build debug version of neard, store-validator and genesis-populate
debug: neard-debug
	cargo build -p store-validator
	cargo build -p genesis-populate
	$(MAKE) sandbox

#? nightly-release: build release version of neard, store-validator and genesis-populate with nightly features
nightly-release: neard-nightly-release
	cargo build -p store-validator --release --features nearcore/nightly
	cargo build -p genesis-populate --release --features nearcore/nightly

neard-nightly-release:
	cargo build -p neard --release --features nightly


#? nightly-debug: build debug version of neard, store-validator and genesis-populate with nightly features
nightly-debug:
	cargo build -p neard --features nightly
	cargo build -p store-validator --features nearcore/nightly
	cargo build -p genesis-populate --features nearcore/nightly

#? assertions-release: build release version of neard with open debug_assertions
assertions-release: NEAR_RELEASE_BUILD=release
assertions-release:
	CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true cargo build -p neard --release

#? sandbox: build debug version of neard with sandbox feature
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

#? test-features-release: build release version of neard with test_features feature
test-features-release: NEAR_RELEASE_BUILD=release
test-features-release:
	cargo build -p neard --release --features test_features


.PHONY: docker-nearcore docker-nearcore-nightly release neard debug
.PHONY: nightly-release nightly-debug assertions-release sandbox
.PHONY: sandbox-release

#? help: get this help message
help: Makefile
	@echo " Choose a command to run:"
	@sed -n 's/^#?//p' $< | column -t -s ':' |  sort | sed -e 's/^/ /'
.PHONY: help
