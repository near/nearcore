docker-nearcore:
	DOCKER_BUILDKIT=1 docker build -t nearcore -f Dockerfile .

docker-nearcore-nightly:
	DOCKER_BUILDKIT=1 docker build -t nearcore-nightly -f Dockerfile.nightly .

release:
	cargo +stable build -p neard --release
	cargo +stable build -p keypair-generator --release
	cargo +stable build -p genesis-csv-to-json --release
	cargo +stable build -p near-vm-runner-standalone --release
	cargo +stable build -p state-viewer --release
	cargo +stable build -p store-validator --release

debug:
	cargo +stable build -p neard
	cargo +stable build -p keypair-generator
	cargo +stable build -p genesis-csv-to-json
	cargo +stable build -p near-vm-runner-standalone
	cargo +stable build -p state-viewer
	cargo +stable build -p store-validator

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
