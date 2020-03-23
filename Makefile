docker-nearcore:
	DOCKER_BUILDKIT=1 docker build -t nearcore -f Dockerfile .

release:
	cargo build -p near --release
	cargo build -p keypair-generator --release
	cargo build -p genesis-csv-to-json --release
	cargo build -p near-vm-runner-standalone --release
	cargo build -p state-viewer --release

debug:
	cargo build -p near
	cargo build -p keypair-generator
	cargo build -p genesis-csv-to-json
	cargo build -p near-vm-runner-standalone
	cargo build -p state-viewer
