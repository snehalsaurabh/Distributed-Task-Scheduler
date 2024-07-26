package schedular

import (
	"context"
	"dts/prisma/db"
	"dts/psm"
	"log"

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
