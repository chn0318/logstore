package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	storagepb "github.com/chn0318/logstore/proto/storagepb"
	"github.com/spf13/viper"

	"github.com/chn0318/logstore/mapservice"
	"github.com/chn0318/logstore/sharedlog/scalog"
	"github.com/chn0318/logstore/storageserver"
)

func main() {
	viper.SetConfigFile("/home/chn/.scalog.yaml")
	if err := viper.ReadInConfig(); err == nil {
		log.Printf("Using config file: %v", viper.ConfigFileUsed())
	}

	logImpl, err := scalog.NewScalogSystem()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
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
