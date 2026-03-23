package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// 命令行参数定义
	address := flag.String("address", "localhost:9092", "Kafka broker address")
	topic := flag.String("topic", "benchmark-topic", "Topic to produce messages to")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers")
	msgSize := flag.Int("msg-size", 1024, "Message size in bytes")
	totalMsgs := flag.Int("num", 100000, "Total number of messages to produce")
	async := flag.Bool("async", true, "Use asynchronous produce mode")
	batchSize := flag.Int("batch", 100, "Producer batch size")

	flag.Parse()

	fmt.Println("----------------------------------------------------------------")
	fmt.Println("🚀 miKago PERFORMANCE BENCHMARK TOOL")
	fmt.Printf("📍 Broker:      %s\n", *address)
	fmt.Printf("📂 Topic:       %s\n", *topic)
	fmt.Printf("📦 Message:     %d bytes\n", *msgSize)
	fmt.Printf("🧵 Concurrency: %d workers\n", *concurrency)
	fmt.Printf("🔢 Total:       %d messages\n", *totalMsgs)
	fmt.Printf("⚡ Mode:        Async=%v, Batch=%d\n", *async, *batchSize)
	fmt.Println("----------------------------------------------------------------")

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(*address),
		Topic:                  *topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		Async:                  *async,
		BatchSize:              *batchSize,
		RequiredAcks:           kafka.RequiredAcks(1),
	}
	defer writer.Close()

	// 准备样本消息
	msgValue := make([]byte, *msgSize)
	for i := 0; i < *msgSize; i++ {
		msgValue[i] = byte(i % 256)
	}

	// 信号监听（按 Ctrl+C 也能看到统计结果）
	exitCh := make(chan os.Signal, 1)
	signal.Notify(exitCh, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup
	msgsPerWorker := *totalMsgs / *concurrency
	latencies := make([]time.Duration, 0, *totalMsgs)
	var latMu sync.Mutex

	start := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < msgsPerWorker; j++ {
				mStart := time.Now()
				err := writer.WriteMessages(context.Background(), kafka.Message{
					Value: msgValue,
				})
				if err != nil {
					log.Printf("Worker %d error: %v", workerID, err)
					return
				}
				
				// 采样收集时延 (只在异步/同步开关开启时有意义)
				latMu.Lock()
				latencies = append(latencies, time.Since(mStart))
				latMu.Unlock()
			}
		}(i)
	}

	// 等待完成
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("✅ All messages produced successfully.")
	case <-exitCh:
		fmt.Println("\n⚠️ Benchmark interrupted by user.")
	}

	elapsed := time.Since(start)
	
	// 结果统计输出
	actualCount := len(latencies)
	qps := float64(actualCount) / elapsed.Seconds()
	throughput := (float64(actualCount*(*msgSize)) / 1024 / 1024) / elapsed.Seconds()

	fmt.Println("\n📊 PERFORMANCE SUMMARY")
	fmt.Printf("⏰ Time elapsed:  %v\n", elapsed)
	fmt.Printf("📈 QPS:           %.2f msgs/sec\n", qps)
	fmt.Printf("🚀 Throughput:    %.2f MB/sec\n", throughput)

	if actualCount > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		fmt.Printf("📉 Latency P50:   %v\n", latencies[actualCount/2])
		fmt.Printf("📉 Latency P90:   %v\n", latencies[actualCount*90/100])
		fmt.Printf("📉 Latency P99:   %v\n", latencies[actualCount*99/100])
	}
	fmt.Println("----------------------------------------------------------------")
}
