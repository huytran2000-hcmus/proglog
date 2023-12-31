CONFIG_PATH=${HOME}/.proglog/
# ==================================================================================== #
# HELPERS
# ==================================================================================== #

## help: print this help message
.PHONY: help
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | sed -e 's/^/ /'

## init: init proglog files
.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

## compile: compile protobuf definition
.PHONY: compile
compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

$(CONFIG_PATH)/model.conf:
	cp testdata/model.conf ${CONFIG_PATH}/model.conf

$(CONFIG_PATH)/policy.csv:
	cp testdata/policy.csv ${CONFIG_PATH}/policy.csv

## test: run all test
.PHONY: test
test: $(CONFIG_PATH)/model.conf $(CONFIG_PATH)/policy.csv
	go test --count 1 -v ./...

## gencert: generate certificate
.PHONY: gencert
gencert:
	cfssl gencert \
		-initca testdata/ca-csr.json | cfssljson -bare ca
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=testdata/ca-config.json \
		-profile=server \
		testdata/server-csr.json | cfssljson -bare server
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=testdata/ca-config.json \
		-profile=client \
		-cn="root" \
		testdata/client-csr.json | cfssljson -bare root-client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=testdata/ca-config.json \
		-profile=client \
		-cn="nobody" \
		testdata/client-csr.json | cfssljson -bare nobody-client
	mv *.pem *.csr ${CONFIG_PATH}

TAG ?= 0.0.1
build-docker:
	docker build -t github.com/huytran2000-hcmus/proglog:$(TAG) --build-arg "APP_VERSION=$(TAG)" .

build-binaries:
	docker buildx build
