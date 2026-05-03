.PHONY: test build deploy all

test:
	go test ./tests/ -timeout 120s

SWAG := $(shell go env GOPATH)/bin/swag

build:
	$(SWAG) init --parseDependency --parseInternal
	bin/gen-notices
	go build ./...

# deploy: pre-flight gate only. Actual push is handled by portfolio-infra.
deploy:
	@test -z "$$(git status --porcelain)" || \
		(echo "ERROR: uncommitted changes — clean working tree required to deploy"; exit 1)
	@$(MAKE) test
	@$(MAKE) build
	@test -z "$$(git status --porcelain)" || \
		(echo "ERROR: git no longer clean after building. Maybe swagger?"; exit 1)

all: test build
