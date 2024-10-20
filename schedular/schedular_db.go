package schedular

import (
	"context"
	"dts/prisma/db"
	"dts/psm"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PrismaLaptopService struct {
	psm.UnimplementedClientServiceServer
	Db *db.PrismaClient
}

type TaskReq struct {
	Command     string
	ScheduledAt string
}

func NewPrismaSchedularService(db *db.PrismaClient) *PrismaLaptopService {
	return &PrismaLaptopService{Db: db}
}

func (Db *PrismaLaptopService) InsertTask(ctx context.Context, req TaskReq) (*db.TasksModel, error) {

	parsedTime, err := time.Parse(time.RFC3339, req.ScheduledAt)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid time format")

	}

	data, err := Db.Db.Tasks.CreateOne(
		db.Tasks.Command.Set(req.Command),
		db.Tasks.ScheduledAt.Set(parsedTime),
	).Exec(ctx)

	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to create task")
	}

	log.Printf("Task created with ID: %v", data)

	return data, err
}

func (Db *PrismaLaptopService) GetTaskStatus(ctx context.Context, id string) (*db.TasksModel, error) {

	data, err := Db.Db.Tasks.FindUnique(
		db.Tasks.ID.Equals(id),
	).Exec(ctx)

	if err != nil {
		return nil, status.Error(codes.NotFound, "Task not found")
	}

	log.Printf("Task found: %v", data)

	return data, nil
}

func (Db *PrismaLaptopService) StoreFile(ctx context.Context, fileID string, fileBuffer []byte) error {

	_, err := Db.Db.Tasks.CreateOne(
		db.Tasks.Command.Set("File"),
		db.Tasks.ScheduledAt.Set(time.Now()),
		db.Tasks.FileContent.Set(fileBuffer),
		db.Tasks.FileID.Set(fileID),
	).Exec(ctx)

	if err != nil {
		return status.Error(codes.Internal, "Failed to store file")
	}

	return nil
}
