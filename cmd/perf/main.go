package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	storagepb "github.com/chn0318/logstore/proto/storagepb"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC server address")
	totalReq := flag.Int("total-requests", 10000, "total number of MultiPut requests")
	concurrency := flag.Int("concurrency", 32, "number of concurrent workers")
	keysPerReq := flag.Int("keys-per-req", 10, "number of keys per MultiPut request")
	valueSize := flag.Int("value-bytes", 4*1024, "value size in bytes")

	flag.Parse()

	log.Printf("MultiPut benchmark start: addr=%s, total=%d, concurrency=%d, keys-per-req=%d, value-bytes=%d\n",
		*addr, *totalReq, *concurrency, *keysPerReq, *valueSize)

	// 1. 建立到 gRPC server 的连接（所有 goroutine 复用一个连接/一个 client）
	conn, err := grpc.Dial(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	client := storagepb.NewStorageClient(conn)

	// 2. 预先构造一个固定长度的 value
	value := make([]byte, *valueSize)
	// 随便填点内容，避免全是 0
	rand.Seed(time.Now().UnixNano())
	for i := range value {
		value[i] = byte(rand.Intn(256))
	}

	// 3. 准备 worker 任务分发
	type job struct {
		id int
	}

	jobs := make(chan job, *totalReq)
	var wg sync.WaitGroup

	// 统计
	var (
		mu        sync.Mutex
		errCount  int
		startTime = time.Now()
	)

	// 4. 启动 worker
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := range jobs {
				// 每个请求使用一个 context，也可以不设超时
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				// 注意：高压测可以改成 context.Background()
				defer cancel()

				// 构造本次 MultiPut 的 key 列表
				kvs := make([]*storagepb.KV, 0, *keysPerReq)
				for i := 0; i < *keysPerReq; i++ {
					// key 做到“基本唯一”，避免太多冲突（其实冲突也没关系，只是更贴近真实负载）
					key := fmt.Sprintf("k-worker-%d-req-%d-key-%d", workerID, j.id, i)
					kvs = append(kvs, &storagepb.KV{
						Key:   key,
						Value: value,
					})
				}

				_, err := client.MultiPut(ctx, &storagepb.MultiPutRequest{
					Kvs: kvs,
				})
				if err != nil {
					mu.Lock()
					errCount++
					mu.Unlock()
				}
			}
		}(w)
	}

	// 5. 投递所有请求
	for i := 0; i < *totalReq; i++ {
		jobs <- job{id: i}
	}
	close(jobs)

	// 6. 等待所有 worker 完成
	wg.Wait()
	elapsed := time.Since(startTime).Seconds()

	// 7. 计算吞吐量
	successReq := *totalReq - errCount
	totalBytes := float64(successReq * (*keysPerReq) * (*valueSize))
	qps := float64(successReq) / elapsed
	mbps := totalBytes / (1024 * 1024) / elapsed

	log.Printf("=== MultiPut benchmark result ===")
	log.Printf("Total requests:      %d", *totalReq)
	log.Printf("Successful requests: %d", successReq)
	log.Printf("Failed requests:     %d", errCount)
	log.Printf("Elapsed time:        %.3f s", elapsed)
	log.Printf("Throughput:          %.2f req/s", qps)
	log.Printf("Data throughput:     %.2f MB/s", mbps)
}
