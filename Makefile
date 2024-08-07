prisma-generate:
	go run github.com/steebchen/prisma-client-go generate dev

migrate :
	go run github.com/steebchen/prisma-client-go migrate dev

run :
	go run main.go

gen :
	protoc --go_out=. --go-grpc_out=. proto/*.proto

make server:
	go run cmd/server/main.go -port 8080

# go get github.com/steebchen/prisma-client-go


# Stop and remove all containers
# sudo docker-compose down --rmi all --volumes --remove-orphans

# Prune unused Docker resources (networks, volumes, images not associated with any container)
# sudo docker system prune --all --volumes --force


# // worker > main.go

# worker.go sendPulse