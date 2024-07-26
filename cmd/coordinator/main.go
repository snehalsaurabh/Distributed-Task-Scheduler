package main

import (
	"dts/config"
	"dts/coordinator"
	"dts/psm"
	"flag"
	"fmt"
	"net"

	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 8081, "the coordinator server port")
	flag.Parse()
	fmt.Println("Starting coordinator on port", *port)

	prismaClient, err := config.ConnectDb()
	if err != nil {
		panic("Connection to DB failed")
	}

	defer prismaClient.Prisma.Disconnect()

	coordinatorService := coordinator.NewCoordinatorService(prismaClient)

	coordinatorService.Start()

	grpcServer := grpc.NewServer()

	psm.RegisterCoordinatorServiceServer(grpcServer, coordinatorService)

	address := fmt.Sprintf("0.0.0.0:%d", *port)
	listener, err := net.Listen("tcp", address)

	if err != nil {
		panic(err)
	}

	err = grpcServer.Serve(listener)
	if err != nil {
		panic(err)
	}
}
