package coordinator

import (
	"context"
	"dts/prisma/db"
	"dts/psm"
	"fmt"
	"log"
	"time"
)

type PrismaCoordinatorService struct {
	psm.UnimplementedCoordinatorServiceServer
	Db *db.PrismaClient
}

func NewPrismaCoordinatorService(db *db.PrismaClient) *PrismaCoordinatorService {
	return &PrismaCoordinatorService{Db: db}
}

func (d *PrismaCoordinatorService) fetchQualifiedData(ctx context.Context) ([]struct {
	ID          string
	Command     string
	FileContent []byte
}, error) {

	var tasks []struct {
		ID          string
		Command     string
		FileContent []byte
	}

	query := `
        SELECT id, command , "fileContent"
		FROM "Tasks"
		WHERE "scheduledAt" < (NOW() + INTERVAL '30 seconds')
		AND "pickedAt" IS NULL
		ORDER BY "scheduledAt"
		FOR UPDATE SKIP LOCKED
    `

	err := d.Db.Prisma.QueryRaw(query).Exec(ctx, &tasks)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func (d *PrismaCoordinatorService) UpdatePickedTask(ctx context.Context, taskId string) error {
	_, err := d.Db.Tasks.FindUnique(
		db.Tasks.ID.Equals(taskId),
	).Update(
		db.Tasks.PickedAt.Set(time.Now()),
	).Exec(ctx)

	if err != nil {
		return fmt.Errorf("failed to update picked task: %w", err)
	}

	return nil
}

func (d *PrismaCoordinatorService) UpdateTaskStatus(ctx context.Context, taskId string, timestamp time.Time, status string) error {

	if status == "started" {
		_, err := d.Db.Tasks.FindUnique(
			db.Tasks.ID.Equals(taskId),
		).Update(
			db.Tasks.StartedAt.Set(timestamp),
		).Exec(ctx)

		log.Print("Task Started !!")

		if err != nil {
			return fmt.Errorf("failed to update task status(StartedAt): %w", err)
		}
	}

	if status == "completed" {
		_, err := d.Db.Tasks.FindUnique(
			db.Tasks.ID.Equals(taskId),
		).Update(
			db.Tasks.CompletedAt.Set(timestamp),
		).Exec(ctx)

		log.Print("Task Completed !!")

		if err != nil {
			return fmt.Errorf("failed to update task status(CompletedAt): %w", err)
		}

	}

	if status == "failed" {
		_, err := d.Db.Tasks.FindUnique(
			db.Tasks.ID.Equals(taskId),
		).Update(
			db.Tasks.FailedAt.Set(timestamp),
		).Exec(ctx)

		log.Print("Task Failed !!")

		if err != nil {
			return fmt.Errorf("failed to update task status(FailedAt): %w", err)
		}
	}

	return nil
}
