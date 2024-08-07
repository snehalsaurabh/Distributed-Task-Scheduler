package main

import (
	"dts/psm"
	"dts/worker"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
)

func main() {

	time.Sleep(50 * time.Second)

	coordinatorAddress := os.Getenv("COORDINATOR_ADDRESS")
	if coordinatorAddress == "" {
		coordinatorAddress = "coordinator:8081" // default value for local testing
		// coordinatorAddress = "localhost:8081" // default value for local testing

	}

	fmt.Printf("Starting worker, connecting to coordinator at %s\n", coordinatorAddress)

	// Let the OS assign a port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	fmt.Printf("Worker server listening on port %d\n", port)

	workerService := worker.NewWorkerService(fmt.Sprintf("%d", port), coordinatorAddress)

	if err := workerService.Start(); err != nil {
		log.Fatalf("Failed to start worker service: %v", err)
	}

	grpcServer := grpc.NewServer()
	psm.RegisterWorkerServiceServer(grpcServer, workerService)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
