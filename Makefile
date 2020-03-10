
usage:
	@echo "make all                  - go-vet, test and build project"
	@echo "make version=v1.0.0 build - build project"
	@echo "make test                 - run unit tests"
	@echo "make go-vet               - go vet"

all: go-vet test build

build:
	@echo "==> Removing old directories..."
	@rm -f bin/*
	@mkdir -p /bin/

	@echo "==> Build:"

# Get the git commit.
	$(eval GIT_COMMIT = $(shell git rev-parse HEAD))
	$(eval GIT_DIRTY = $(shell test -n "`git status --porcelain`" && echo "+CHANGES" || true))

# Get the version of Go.
	$(eval GO_VERSION = "$(shell go version | awk '{print $$3}')")
	$(eval PLATFORM = "$(shell go version | awk '{print $$4}')")

# Get the build date.
	$(eval BUILD_DATE = "$(shell date -u --rfc-3339=second | sed 's/ /T/g')")

# Override the variables in the cmd package.
	$(eval V_PKG = "github.com/Huuancao/sentinel/cmd")
	$(eval LD_FLAGS = "-X ${V_PKG}.version=${version} -X ${V_PKG}.gitCommit=${GIT_COMMIT}${GIT_DIRTY} -X ${V_PKG}.buildDate=${BUILD_DATE} ${LD_FLAGS} -X ${V_PKG}.goVersion=${GO_VERSION} -X ${V_PKG}.platform=${PLATFORM}")

	@go build -ldflags ${LD_FLAGS} -o ./bin/sentinel
	@echo "Built commit '${GIT_COMMIT}${GIT_DIRTY}' for version '${version}'."
	@ls -hl bin/

go-vet:
	@echo "==> Go vet:"
	@go vet ./...

test:
	@echo "==> Go test:"
	@go test --race ./...

coverage:
# Output test coverage for all packages in profile.cov
	@go test -covermode=set -coverprofile=profile.cov ./...
# Coveralls.io integration.
	@goveralls -coverprofile=profile.cov -service=travis-ci 