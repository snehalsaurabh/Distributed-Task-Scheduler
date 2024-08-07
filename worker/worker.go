package worker

import (
	"context"
	"dts/psm"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const numOfWorkers = 5

type WorkerService struct {
	psm.UnimplementedWorkerServiceServer
	// grpcServer          *grpc.Server
	id                 uint32
	serverPort         string
	coodinatorAddress  string
	coordinatorConn    *grpc.ClientConn
	coordinatorClient  psm.CoordinatorServiceClient
	pulseInterval      uint32
	taskQueue          chan *psm.SubmitTaskRequest
	ReceivedTasks      map[string]*psm.SubmitTaskRequest
	ReceivedTasksMutex *sync.Mutex
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 *sync.WaitGroup
}

func NewWorkerService(port string, coordinatorAddress string) *WorkerService {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerService{
		id:                 uuid.New().ID(),
		serverPort:         port,
		coodinatorAddress:  coordinatorAddress,
		pulseInterval:      5,
		taskQueue:          make(chan *psm.SubmitTaskRequest, 100),
		ReceivedTasks:      make(map[string]*psm.SubmitTaskRequest),
		ReceivedTasksMutex: &sync.Mutex{},
		ctx:                ctx,
		cancel:             cancel,
		wg:                 &sync.WaitGroup{},
	}
}

func (w *WorkerService) Start() error {
	w.startWorkers()

	if err := w.connectToCoordinator(); err != nil {
		fmt.Printf("Worker : %s failed to connect to coordinator", string(w.id))
		return err
	}

	go w.pulse()
	// Start task processing
	return nil
}

func (w *WorkerService) connectToCoordinator() error {
	log.Printf("Worker : %s connecting to coordinator at %s", string(w.id), w.coodinatorAddress)
	var err error
	w.coordinatorConn, err = grpc.Dial(w.coodinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Worker : %s failed to connect to coordinator at %s", string(w.id), w.coodinatorAddress)
	}

	w.coordinatorClient = psm.NewCoordinatorServiceClient(w.coordinatorConn)
	log.Printf("Worker : %s connected to coordinator at %s", string(w.id), w.coodinatorAddress)
	return nil
}

func (w *WorkerService) startWorkers() {
	for i := 0; i < numOfWorkers; i++ {
		go w.worker()
	}
}

func (w *WorkerService) SubmitTask(ctx context.Context, req *psm.SubmitTaskRequest) (*psm.SubmitTaskResponse, error) {
	log.Printf("Worker : %s received task %s", string(w.id), req.TaskId)

	// w.ReceivedTasksMutex.Lock()
	// w.ReceivedTasks[req.GetTaskId()] = req
	// w.ReceivedTasksMutex.Unlock()

	w.taskQueue <- req

	return &psm.SubmitTaskResponse{
		Message: "Task was submitted",
		Success: true,
		TaskId:  req.TaskId,
	}, nil
}

func (w *WorkerService) worker() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case task := <-w.taskQueue:
			go w.updateStatus(task, psm.TaskStatus_STARTED)
			w.processTask(task)
			go w.updateStatus(task, psm.TaskStatus_COMPLETED)

		}
	}
}

func (w *WorkerService) processTask(task *psm.SubmitTaskRequest) {
	log.Printf("Worker : %s processing task %s", string(w.id), task.TaskId)

	re := regexp.MustCompile(`^\s*curl\b`)

	if re.MatchString(task.Data) {
		w.executeCurlCommand(task.Data)
	} else {
		w.executeBashCommand(task)
	}

	log.Printf("Worker : %s processed task %s", string(w.id), task.TaskId)
}

func (w *WorkerService) executeBashCommand(task *psm.SubmitTaskRequest) {
	cmd := exec.Command("bash", "-c", task.Data)

	// Direct the command's output to the standard output and error streams
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Run the command
	err := cmd.Run()
	if err != nil {
		log.Printf("Worker : %s failed to process task %s with error: %s", string(w.id), task.TaskId, err)
		return
	}
}

func (w *WorkerService) executeCurlCommand(command string) {
	cmd := exec.Command("bash", "-c", command)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("Worker : %s failed to execute curl command: %v", string(w.id), err)
	}
}

func (w *WorkerService) updateStatus(task *psm.SubmitTaskRequest, status psm.TaskStatus) {
	_, err := w.coordinatorClient.UpdateTaskStatus(w.ctx, &psm.UpdateTaskStatusRequest{
		TaskId:      task.TaskId,
		Status:      status,
		StartedAt:   time.Now().Unix(),
		CompletedAt: time.Now().Unix(),
	})

	if err != nil {
		log.Printf("Worker : %s failed to update task status", string(w.id))
	}

}

func (w *WorkerService) pulse() {
	w.wg.Add(1)
	defer w.wg.Done()

	beeper := time.NewTicker(time.Duration(w.pulseInterval) * time.Second)
	defer beeper.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-beeper.C:
			err := w.sendPulse()
			if err != nil {
				log.Printf("Worker : %v failed to pulse", err)
				return
			}
		}
	}
}

func (w *WorkerService) sendPulse() error {
	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		// If not set, use localhost with the dynamically assigned port
		workerAddress = fmt.Sprintf("worker:%s", w.serverPort)
		// workerAddress = fmt.Sprintf("localhost:%s", w.serverPort)
	}

	_, err := w.coordinatorClient.SendPulse(context.Background(), &psm.SendPulseRequest{
		WorkerId: uint64(w.id),
		Address:  workerAddress,
	})
	return err
}

// export COORDINATOR_ADDRESS=localhost:8081
// go run cmd/worker/main.go
