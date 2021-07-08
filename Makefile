proto:
	protoc --go_out=plugins=grpc:. *.proto

lint:
	golangci-lint run