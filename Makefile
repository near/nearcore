docker-nearcore:
	DOCKER_BUILDKIT=1 docker build -t nearcore -f Dockerfile .

docker-nearcore-nightly:
	DOCKER_BUILDKIT=1 docker build -t nearcore-nightly -f Dockerfile.nightly .

RUST_OPTIONS:=+stable

release:
	cargo $(RUST_OPTIONS) build -p neard --release
	cargo $(RUST_OPTIONS) build -p keypair-generator --release
	cargo $(RUST_OPTIONS) build -p genesis-csv-to-json --release
	cargo $(RUST_OPTIONS) build -p near-vm-runner-standalone --release
	cargo $(RUST_OPTIONS) build -p state-viewer --release
	cargo $(RUST_OPTIONS) build -p store-validator --release

debug:
	cargo $(RUST_OPTIONS) build -p neard
	cargo $(RUST_OPTIONS) build -p keypair-generator
	cargo $(RUST_OPTIONS) build -p genesis-csv-to-json
	cargo $(RUST_OPTIONS) build -p near-vm-runner-standalone
	cargo $(RUST_OPTIONS) build -p state-viewer
	cargo $(RUST_OPTIONS) build -p store-validator

perf-release:
	cargo $(RUST_OPTIONS) build -p neard --release --features performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p keypair-generator --release --features performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p genesis-csv-to-json --release --features performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p near-vm-runner-standalone --release --features performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p state-viewer --release --features performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p store-validator --release --features performance_stats,memory_stats

perf-debug:
	cargo $(RUST_OPTIONS) build -p neard --features performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p keypair-generator --features performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p genesis-csv-to-json --features performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p near-vm-runner-standalone --features performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p state-viewer --features performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p store-validator --features performance_stats,memory_stats

nightly-release:
	cargo $(RUST_OPTIONS) build -p neard --release --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p keypair-generator --release --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p genesis-csv-to-json --release --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p near-vm-runner-standalone --release --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p state-viewer --release --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p store-validator --release --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats

nightly-debug:
	cargo $(RUST_OPTIONS) build -p neard --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p keypair-generator --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p genesis-csv-to-json --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p near-vm-runner-standalone --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p state-viewer --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
	cargo $(RUST_OPTIONS) build -p store-validator --features nightly_protocol,nightly_protocol_features,performance_stats,memory_stats
