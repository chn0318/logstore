package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	storagepb "github.com/chn0318/logstore/proto/storagepb"
)

func main() {
	conn, err := grpc.Dial("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	client := storagepb.NewStorageClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Println("=== MultiPut ===")
	_, err = client.MultiPut(ctx, &storagepb.MultiPutRequest{
		Kvs: []*storagepb.KV{
			{Key: "k1", Value: []byte("v1")},
			{Key: "k2", Value: []byte("v2")},
			{Key: "k3", Value: []byte("v3")},
		},
	})
	if err != nil {
		log.Fatalf("MultiPut error: %v", err)
	}
	log.Println("MultiPut OK")

	log.Println("=== MultiGet ===")
	getResp, err := client.MultiGet(ctx, &storagepb.MultiGetRequest{
		Keys: []string{"k1", "k2", "k3", "k-not-exist"},
	})
	if err != nil {
		log.Fatalf("MultiGet error: %v", err)
	}

	for k, v := range getResp.Values {
		log.Printf("key=%s, value=%s\n", k, string(v))
	}
}
