docker-nearcore:
	DOCKER_BUILDKIT=1 docker build -t nearcore -f Dockerfile .

docker-nearcore-nightly:
	DOCKER_BUILDKIT=1 docker build -t nearcore-nightly -f Dockerfile.nightly .

release:
	cargo build -p neard --release
	cargo build -p keypair-generator --release
	cargo build -p genesis-csv-to-json --release
	cargo build -p near-vm-runner-standalone --release
	cargo build -p state-viewer --release
	cargo build -p store-validator --release

debug:
	cargo build -p neard
	cargo build -p keypair-generator
	cargo build -p genesis-csv-to-json
	cargo build -p near-vm-runner-standalone
	cargo build -p state-viewer
	cargo build -p store-validator

nightly-release:
	cargo build -p neard --release --features nightly_protocol --features nightly_protocol_features
	cargo build -p keypair-generator --release --features nightly_protocol --features nightly_protocol_features
	cargo build -p genesis-csv-to-json --release --features nightly_protocol --features nightly_protocol_features
	cargo build -p near-vm-runner-standalone --release --features nightly_protocol --features nightly_protocol_features
	cargo build -p state-viewer --release --features nightly_protocol --features nightly_protocol_features
	cargo build -p store-validator --release --features nightly_protocol --features nightly_protocol_features

nightly-debug:
	cargo build -p neard --features nightly_protocol --features nightly_protocol_features
	cargo build -p keypair-generator --features nightly_protocol --features nightly_protocol_features
	cargo build -p genesis-csv-to-json --features nightly_protocol --features nightly_protocol_features
	cargo build -p near-vm-runner-standalone --features nightly_protocol --features nightly_protocol_features
	cargo build -p state-viewer --release nightly_protocol --features nightly_protocol_features
	cargo build -p store-validator --features nightly_protocol --features nightly_protocol_features
