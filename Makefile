LDFLAGS      := -w -s
GO           := go
BINDIR       := $(CURDIR)/bin


all: build

build:
	GOBIN=$(BINDIR) CGO_CFLAGS=$(CGO_CFLAGS) CGO_LDFLAGS=$(CGO_LDFLAGS) $(GO) install -ldflags '$(LDFLAGS)' ./cmd/...
