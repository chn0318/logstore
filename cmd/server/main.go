package main

import (
	"log"
	"net"

	"google.golang.org/grpc"

	storagepb "github.com/chn0318/logstore/proto/storagepb"

	"github.com/chn0318/logstore/mapservice"
	"github.com/chn0318/logstore/sharedlog/memorylog"
	"github.com/chn0318/logstore/storageserver"
)

func main() {

	logImpl := memorylog.NewMemoryLog()
	ms := mapservice.NewMapService()

	storageSrv := storageserver.NewStorageServer(logImpl, ms)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}

	grpcServer := grpc.NewServer()
	storagepb.RegisterStorageServer(grpcServer, storageSrv)

	log.Println("storage gRPC server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve error: %v", err)
	}
}
