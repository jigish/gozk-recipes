PROJECT_ROOT := $(shell pwd)
VENDOR_PATH  := $(PROJECT_ROOT)/vendor

GOPATH := $(VENDOR_PATH)
export GOPATH

GOCOV := $(VENDOR_PATH)/bin/gocov
export GOCOV

all: test

clean:
	@rm -rf bin pkg

run: clean
	@go run main.go

install:
	@echo "Installing Dependencies..."
	@rm -rf $(VENDOR_PATH)
	@mkdir -p $(VENDOR_PATH) || exit 2
	@GOPATH=$(VENDOR_PATH) go get github.com/axw/gocov/gocov
	@GOPATH=$(VENDOR_PATH) go get launchpad.net/gozk
	@GOPATH=$(VENDOR_PATH) go get launchpad.net/gocheck
	@echo "Done."

test: clean
	@go test

coverage: clean
	@$(GOCOV) test | $(GOCOV) report

annotate: clean
	@$(GOCOV) test >coverage.json
	@$(GOCOV) annotate coverage.json
	@rm -f coverage.json

fmt:
	@find . -name \*.go -exec gofmt -l -w {} \;
	@gofmt -l -w main.go

