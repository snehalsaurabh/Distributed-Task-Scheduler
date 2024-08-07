package main

import (
	"context"
	"dts/psm"
	"flag"
	"fmt"
	"time"

	"google.golang.org/grpc"
)

func main() {
	serverAddress := flag.String("address", "", "The server address in the format of host:port")
	flag.Parse()
	fmt.Println("Dial server on address", *serverAddress)

	conn, err := grpc.Dial(*serverAddress, grpc.WithInsecure())

	if err != nil {
		panic(err)
	}

	laptopClient := psm.NewClientServiceClient(conn)

	fmt.Println("laptopClient", laptopClient)
	// GetStatus(laptopClient)
	CreateTask2(laptopClient)
	// CreateTask(laptopClient)
	// CreateTask(laptopClient)
	// CreateTask(laptopClient)
	// CreateTask(laptopClient)

}

func GetStatus(laptopClient psm.ClientServiceClient) {

	req := &psm.GetStatusRequest{
		TaskId: "d077b1b3-9862-4163-b01e-1d660cc4505f",
	}

	res, err := laptopClient.GetStatus(context.Background(), req)
	if err != nil {
		panic(err)
	}

	fmt.Println("Task status\n", res)
}

func CreateTask(laptopClient psm.ClientServiceClient) {

	req := &psm.ScheduleTaskRequest{
		Command:     "echo Hello World",
		ScheduledAt: time.Now().Format(time.RFC3339),
	}

	res, err := laptopClient.ScheduleTask(context.Background(), req)
	if err != nil {
		panic(err)
	}

	fmt.Println("Task created\n", res)
}

func CreateTask2(laptopClient psm.ClientServiceClient) {

	req := &psm.ScheduleTaskRequest{
		Command:     `for i in {1..5}; do echo "Loop $i"; done`,
		ScheduledAt: time.Now().Format(time.RFC3339),
	}

	res, err := laptopClient.ScheduleTask(context.Background(), req)
	if err != nil {
		panic(err)
	}

	fmt.Println("Task created\n", res)
}
