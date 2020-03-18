docker-nearcore:
	DOCKER_BUILDKIT=1 docker build -t nearcore -f Dockerfile .

release:
	cargo build -p near --release
	cargo build -p keypair-generator --release
	cargo build -p genesis-csv-to-json --release

debug:
	cargo build -p near
	cargo build -p keypair-generator
	cargo build -p genesis-csv-to-json
