LDFLAGS      := -w -s
ROCKSDB_PATH := $(shell brew --prefix rocksdb || echo "$(HOME)/Cellar/rocksdb/6.7.3")
CGO_CFLAGS   := -I$(ROCKSDB_PATH)/include
CGO_LDFLAGS  := -L$(ROCKSDB_PATH)/lib

## CGO_CFLAGS   = -I$(HOME)/Cellar/rocksdb/6.7.3/include
## CGO_LDFLAGS  = -L$(HOME)/Cellar/rocksdb/6.7.3/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd
GO           := go
BINDIR       := $(CURDIR)/bin


all: build

build:
	GOBIN=$(BINDIR) CGO_CFLAGS=$(CGO_CFLAGS) CGO_LDFLAGS=$(CGO_LDFLAGS) $(GO) install -ldflags '$(LDFLAGS)' ./cmd/...
