package schedular

import (
	"context"
	"dts/prisma/db"
	"dts/psm"
	"io"
	"log"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type schedularService struct {
	psm.UnimplementedClientServiceServer
	Db *db.PrismaClient
}

func NewSchedularService(db *db.PrismaClient) *schedularService {
	return &schedularService{Db: db}
}

func (s *schedularService) ScheduleTask(ctx context.Context, req *psm.ScheduleTaskRequest) (*psm.ScheduleTaskResponse, error) {

	db_service := NewPrismaSchedularService(s.Db)

	task, err := db_service.InsertTask(ctx, TaskReq{Command: req.Command, ScheduledAt: req.ScheduledAt})
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to create task")
	}

	return &psm.ScheduleTaskResponse{
		TaskId:      task.ID,
		Command:     task.Command,
		ScheduledAt: task.ScheduledAt.Unix(),
	}, nil
}

func (s *schedularService) GetStatus(ctx context.Context, req *psm.GetStatusRequest) (*psm.GetStatusResponse, error) {

	db_service := NewPrismaSchedularService(s.Db)

	task, err := db_service.GetTaskStatus(ctx, req.TaskId)
	if err != nil {
		return nil, status.Error(codes.NotFound, "Task not found")
	}

	// return &psm.GetStatusResponse{
	// 	TaskId:      task.ID,
	// 	Command:     task.Command,
	// 	ScheduledAt: task.ScheduledAt.String(),
	// 	FailedAt:    task.FailedAt.String(),
	// }, nil

	log.Printf("Task found: %v", task.ID)

	return &psm.GetStatusResponse{
		TaskId:      task.ID,
		Command:     task.Command,
		ScheduledAt: task.ScheduledAt.String(),
	}, nil
}

func (s *schedularService) TransferFile(stream psm.ClientService_TransferFileServer) error {
	db_service := NewPrismaSchedularService(s.Db)
	var fileBuffer []byte
	var filename string

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "Error receiving file chunk: %v", err)
		}

		if filename == "" {
			filename = chunk.Filename
		}

		fileBuffer = append(fileBuffer, chunk.Content...)

		if chunk.IsLast {
			break
		}
	}

	fileID := uuid.New().String()

	err := db_service.StoreFile(stream.Context(), fileID, fileBuffer)
	if err != nil {
		return status.Errorf(codes.Internal, "Error storing file: %v", err)
	}

	response := &psm.FileTransferResponse{
		Success: true,
		Message: "File received and stored successfully",
		FileId:  fileID,
	}

	return stream.SendAndClose(response)
}
