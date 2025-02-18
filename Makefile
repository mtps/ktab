LDFLAGS      := -w -s
GO           := go
BINDIR       := $(CURDIR)/bin

all: build

build:
	mkdir -p $(BINDIR)/
	$(GO) build -o $(BINDIR) ./cmd/...

docker:
	docker build -t mtps/ktab .

docker-push: docker
	docker push mtps/ktab
