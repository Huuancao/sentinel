
usage:
	@echo "make all    - go-vet, test and build project"
	@echo "make build  - build project"
	@echo "make test   - run unit tests"
	@echo "make go-vet - go vet"

all: go-vet test build

build:
	@echo "==> Removing old directories..."
	@rm -f bin/*
	@mkdir -p /bin/

	@echo "==> Build:"
	go build -o ./bin/sentinel
	@echo "Built commit '${GIT_COMMIT}${GIT_DIRTY}' for version '${version}'."
	@ls -hl bin/

go-vet:
	@echo "==> Go vet:"
	@go vet ./...

test:
	@echo "==> Go test:"
	@go test --race ./...
