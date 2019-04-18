
docker-nearmint:
	TM_EXISTS=$(shell [ -e target/tendermint/tendermint ] && echo 1 || echo 0 )
	@if [ ! -e target/tendermint/tendermint ]; then\
		mkdir -p target/tendermint; \
		wget https://github.com/tendermint/tendermint/releases/download/v0.31.5/tendermint_v0.31.5_linux_amd64.zip -O target/tendermint/tendermint.zip; \
		unzip -o target/tendermint/tendermint.zip -d target/tendermint/; \
	fi
	DOCKER_BUILDKIT=1 docker build -t nearmint -f nearmint/ops/Dockerfile .

