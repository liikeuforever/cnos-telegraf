package cnosdb

//go:generate flatc -o internal/models --go --go-namespace models --gen-onefile ./internal/models/models.fbs
//go:generate protoc --go_out=./internal --go-grpc_out=./internal ./internal/service/kv_service.proto
