package coordinator

import (
	"context"
	"dts/prisma/db"
	"dts/psm"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const scanInterval = 5 * time.Second

type CoordinatorService struct {
	psm.UnimplementedCoordinatorServiceServer
	Db *db.PrismaClient
	// grpcServer          *grpc.Server
	WorkerPool          map[uint64]*Worker
	WorkerPoolMutex     *sync.Mutex
	WorkerPoolKeys      []uint64
	WorkerPoolKeysMutex *sync.Mutex
	maxPulseMisses      uint8
	pulseInterval       time.Duration
	roundRobinIndex     uint32
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  *sync.WaitGroup
}

type Worker struct {
	WorkerId     uint64
	pulseMisses  uint8
	Conn         *grpc.ClientConn
	WorkerClient psm.WorkerServiceClient
}

func NewCoordinatorService(db *db.PrismaClient) *CoordinatorService {
	ctx, cancel := context.WithCancel(context.Background())
	return &CoordinatorService{
		WorkerPool:          make(map[uint64]*Worker),
		WorkerPoolMutex:     &sync.Mutex{},
		WorkerPoolKeys:      []uint64{},
		WorkerPoolKeysMutex: &sync.Mutex{},
		maxPulseMisses:      2,
		pulseInterval:       5 * time.Second,
		roundRobinIndex:     0,
		ctx:                 ctx,
		cancel:              cancel,
		wg:                  &sync.WaitGroup{},
		Db:                  db,
	}
}

func (c *CoordinatorService) Start() {
	go c.ManageWorkerPool()

	go c.scanDatabase()
}

func (c *CoordinatorService) ManageWorkerPool() {
	c.wg.Add(1)
	defer c.wg.Done()

	beeper := time.NewTicker(time.Duration(scanInterval))
	defer beeper.Stop()
	for {
		select {
		case <-beeper.C:
			c.disconnectInactiveWorkers()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *CoordinatorService) disconnectInactiveWorkers() {
	c.WorkerPoolMutex.Lock()
	defer c.WorkerPoolMutex.Unlock()

	for workerId, worker := range c.WorkerPool {
		if worker.pulseMisses > c.maxPulseMisses {
			worker.Conn.Close()
			delete(c.WorkerPool, workerId)

			c.WorkerPoolKeysMutex.Lock()
			c.WorkerPoolKeys = make([]uint64, 0, len(c.WorkerPool))
			for workerId := range c.WorkerPool {
				c.WorkerPoolKeys = append(c.WorkerPoolKeys, workerId)
			}

			log.Printf("Worker with id: %d removed from the worker pool", workerId)
			c.WorkerPoolKeysMutex.Unlock()
		} else {
			worker.pulseMisses++
		}
	}
}

func (c *CoordinatorService) SendPulse(ctx context.Context, pulse *psm.SendPulseRequest) (*psm.SendPulseResponse, error) {
	c.WorkerPoolMutex.Lock()
	defer c.WorkerPoolMutex.Unlock()

	workerId := pulse.GetWorkerId()
	if worker, ok := c.WorkerPool[workerId]; ok {
		worker.pulseMisses = 0
	} else {
		log.Print("Worker with id: ", workerId, " is not in the worker pool")

		log.Printf("Pulse Address : %v", pulse.GetAddress())

		connection, err := grpc.Dial(pulse.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			return nil, errors.New("Failed to connect to worker with id: " + fmt.Sprint(workerId))
		}

		newWorker := &Worker{
			WorkerId:     workerId,
			pulseMisses:  0,
			Conn:         connection,
			WorkerClient: psm.NewWorkerServiceClient(connection),
		}

		c.WorkerPool[workerId] = newWorker
		c.WorkerPoolKeysMutex.Lock()

		c.WorkerPoolKeys = make([]uint64, 0, len(c.WorkerPool))
		for workerId := range c.WorkerPool {
			c.WorkerPoolKeys = append(c.WorkerPoolKeys, workerId)
		}
		defer c.WorkerPoolKeysMutex.Unlock()

		log.Print("Worker with id: ", workerId, " added to the worker pool")

	}
	return &psm.SendPulseResponse{Affirmed: true}, nil
}

func (c *CoordinatorService) scanDatabase() {
	beeper := time.NewTicker(time.Duration(scanInterval))
	defer beeper.Stop()

	for {
		select {
		case <-beeper.C:
			c.scanDatabaseForTasks()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *CoordinatorService) scanDatabaseForTasks() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db_client := NewPrismaCoordinatorService(c.Db)

	tasks, err := db_client.fetchQualifiedData(ctx)

	if err != nil {
		log.Print("Failed to fetch tasks: ", err)
		return
	}

	for _, task := range tasks {
		// log.Print("Task found: ", task.ID)
		formattedTask := &psm.SubmitTaskRequest{TaskId: task.ID, Data: task.Command}
		// log.Printf("Formatted Task : %v", formattedTask)
		if err := c.submitTaskToWorker(ctx, formattedTask); err != nil {
			log.Print("Failed to submit task: ", err)
			continue
		}

		if err := db_client.UpdatePickedTask(ctx, task.ID); err != nil {
			log.Print("Failed to update picked task: ", err)
			continue
		}
	}
}

func (c *CoordinatorService) submitTaskToWorker(ctx context.Context, task *psm.SubmitTaskRequest) error {

	worker := c.getNextWorker()
	if worker == nil {
		return errors.New("no worker available")
	}

	log.Printf("Submitting task %v to worker %d", worker, worker.WorkerId)

	_, err := worker.WorkerClient.SubmitTask(ctx, task)
	if err != nil {
		// log.Printf("Failed to submit task to worker IDK Why: %v", err)
		return err
	}
	return nil
}

func (c *CoordinatorService) PrintWorkerPool() {
}

func (c *CoordinatorService) getNextWorker() *Worker {
	c.WorkerPoolKeysMutex.Lock()
	defer c.WorkerPoolKeysMutex.Unlock()

	workerCount := len(c.WorkerPoolKeys)
	if workerCount == 0 {
		return nil
	}

	worker := c.WorkerPool[c.WorkerPoolKeys[c.roundRobinIndex%uint32(workerCount)]]
	c.roundRobinIndex++
	return worker
}

func (c *CoordinatorService) UpdateTaskStatus(ctx context.Context, req *psm.UpdateTaskStatusRequest) (*psm.UpdateTaskStatusResponse, error) {
	phase := req.GetStatus()
	taskId := req.GetTaskId()

	db_client := NewPrismaCoordinatorService(c.Db)

	var err error

	switch phase {
	case psm.TaskStatus_STARTED:
		err = db_client.UpdateTaskStatus(ctx, taskId, time.Unix(req.GetStartedAt(), 0), "started")
	case psm.TaskStatus_COMPLETED:
		err = db_client.UpdateTaskStatus(ctx, taskId, time.Unix(req.GetCompletedAt(), 0), "completed")
	case psm.TaskStatus_FAILED:
		err = db_client.UpdateTaskStatus(ctx, taskId, time.Unix(req.GetFailedAt(), 0), "failed")
	default:
		return nil, status.Errorf(codes.InvalidArgument, "Invalid status")
	}

	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to update task status: %v", err)
	}

	return &psm.UpdateTaskStatusResponse{
		Success: true,
	}, nil
}
