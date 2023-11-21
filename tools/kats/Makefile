PROJECT=kats
SOURCES=$(shell find . -name "*.go" \! -name "*_test.go" -print)
SOURCES_TEST=$(shell find . -name "*_test.go" -print)
GIT_TAG_1=$(shell git describe --abbrev=1 || echo "v0.0.0")
BUILD_FLAGS=${GO_BUILD_FLAGS}

LINK_FLAGS="-X main.ProjectName=$(PROJECT) -X main.GitTag=$(GIT_TAG_1)"

.PHONY: test benchmark build 

#go.sum: go.mod $(SOURCES)
#	touch $@
#	go mod tidy

build: $(SOURCES)
	go build $(BUILD_FLAGS) -ldflags $(LINK_FLAGS) .

test:
	go test $(BUILD_FLAGS) .

benchmaek: build
	go test $(BUILD_FLAGS) -bench .

