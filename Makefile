
docker-nearmint:
	mkdir -p target/tendermint
	wget https://github.com/tendermint/tendermint/releases/download/v0.31.5/tendermint_v0.31.5_linux_amd64.zip -O target/tendermint/tendermint.zip
	unzip -o target/tendermint/tendermint.zip -d target/tendermint/
	docker build -t nearmint -f nearmint/ops/Dockerfile .
