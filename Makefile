# Build directory
BUILD_DIR := ./build

# Git commit for version info
GIT_COMMIT := $(shell git rev-parse HEAD)

# Build flags
BUILD_FLAGS := -buildvcs=false
LDFLAGS := -X vsc-node/modules/announcements.GitCommit=$(GIT_COMMIT)

# Installation directory (default to /usr/local/bin)
PREFIX ?= /usr/local
INSTALL_DIR := $(PREFIX)/bin

# GraphQL schema and generated outputs
GQL_SCHEMA := modules/gql/schema.graphql
GQL_GENERATED := modules/gql/gqlgen/generated.go

# Source dependencies
GO_SOURCES := $(shell find modules lib -type f -name '*.go') go.mod go.sum

# Targets
.PHONY: all clean install magid contract-deployer genesis-elector devnet-setup mapping-bot generate \
	regression regression-smoke regression-tss regression-blame regression-edge regression-gossip regression-list

all: $(GQL_GENERATED) magid contract-deployer genesis-elector devnet-setup mapping-bot

generate: $(GQL_GENERATED)

$(GQL_GENERATED): $(GQL_SCHEMA)
	go run github.com/99designs/gqlgen generate

magid: $(BUILD_DIR)/magid
	@echo "Built magid"

contract-deployer: $(BUILD_DIR)/contract-deployer
	@echo "Built contract-deployer"

genesis-elector: $(BUILD_DIR)/genesis-elector
	@echo "Built genesis-elector"

devnet-setup: $(BUILD_DIR)/devnet-setup
	@echo "Built devnet-setup"

mapping-bot: $(BUILD_DIR)/mapping-bot
	@echo "Built mapping-bot"

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/magid: $(BUILD_DIR) $(GO_SOURCES)
	go build $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" -o $@ vsc-node/cmd/vsc-node

$(BUILD_DIR)/contract-deployer: $(BUILD_DIR) $(GO_SOURCES)
	go build $(BUILD_FLAGS) -o $@ vsc-node/cmd/contract-deployer

$(BUILD_DIR)/genesis-elector: $(BUILD_DIR) $(GO_SOURCES)
	go build $(BUILD_FLAGS) -o $@ vsc-node/cmd/genesis-elector

$(BUILD_DIR)/devnet-setup: $(BUILD_DIR) $(GO_SOURCES)
	go build $(BUILD_FLAGS) -o $@ vsc-node/cmd/devnet-setup

$(BUILD_DIR)/mapping-bot: $(BUILD_DIR) $(GO_SOURCES)
	go build $(BUILD_FLAGS) -o $@ vsc-node/cmd/mapping-bot

install: all
	mkdir -p $(INSTALL_DIR)
	cp $(BUILD_DIR)/magid $(INSTALL_DIR)/
	cp $(BUILD_DIR)/contract-deployer $(INSTALL_DIR)/
	cp $(BUILD_DIR)/genesis-elector $(INSTALL_DIR)/
	cp $(BUILD_DIR)/devnet-setup $(INSTALL_DIR)/
	cp $(BUILD_DIR)/mapping-bot $(INSTALL_DIR)/

clean:
	rm -rf $(BUILD_DIR)

# ---- Regression / devnet integration tests --------------------------------
# Docker-based integration suite under tests/devnet/. Heavy: needs Docker,
# ~20GB free disk, multi-GB RAM. The full run is ~3-4 hours.
# See tests/devnet/README.md for per-test descriptions.
#
# Override REGRESSION_FILTER / REGRESSION_TIMEOUT to scope a custom run, e.g.
#   make regression REGRESSION_FILTER=TestTSSReshareHappyPath REGRESSION_TIMEOUT=25m

REGRESSION_FILTER  ?= TestTSS|TestBlame|TestEdge|TestGossip
REGRESSION_TIMEOUT ?= 300m

regression:
	go test -v -run '$(REGRESSION_FILTER)' -timeout $(REGRESSION_TIMEOUT) ./tests/devnet/

regression-smoke:
	go test -v -run 'TestDevnetSetup|TestDeployCallTss' -timeout 30m ./tests/devnet/

regression-tss:
	go test -v -run 'TestTSS' -timeout 120m ./tests/devnet/

regression-blame:
	go test -v -run 'TestBlame' -timeout 90m ./tests/devnet/

regression-edge:
	go test -v -run 'TestEdge' -timeout 120m ./tests/devnet/

regression-gossip:
	go test -v -run 'TestGossip' -timeout 30m ./tests/devnet/

regression-list:
	@cd tests/devnet && grep -h '^func Test' *_test.go | awk '{print $$2}' | sed 's/(.*//' | sort
