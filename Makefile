docker-nearcore:
	DOCKER_BUILDKIT=1 docker build -t nearcore-dev -f Dockerfile .
	mkdir -p docker-build
	docker run -v ${PWD}/docker-build:/opt/mount --rm --entrypoint cp nearcore-dev /usr/local/bin/near /opt/mount/near
	docker run -v ${PWD}/docker-build:/opt/mount --rm --entrypoint cp nearcore-dev /usr/local/bin/genesis-csv-to-json /opt/mount/genesis-csv-to-json
	docker run -v ${PWD}/docker-build:/opt/mount --rm --entrypoint cp nearcore-dev /usr/local/bin/keypair-generator /opt/mount/keypair-generator
	docker build -t nearcore -f Dockerfile.prod .
