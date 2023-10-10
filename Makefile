build:
	go build -o ./bin/cosmos-gc ./cmd/cosmos-gc/main.go

install:
	go install ./cmd/cosmos-gc
