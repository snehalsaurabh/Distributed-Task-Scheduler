package worker

import (
	"context"
	"dts/psm"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
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
	id                uint32
	serverPort        string
	coodinatorAddress string
	coordinatorConn   *grpc.ClientConn
	coordinatorClient psm.CoordinatorServiceClient
	pulseInterval     uint32
	taskQueue         chan *psm.SubmitTaskRequest
	// fileQueue          chan *psm.SubmitFileRequest
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
	re2 := regexp.MustCompile(`^\s*(git clone|cd|node)\b`)
	rePython := regexp.MustCompile(`^\s*(python|python3)\b`)

	if re.MatchString(task.Data) {
		w.executeCurlCommand(task.Data)
	} else if re2.MatchString(task.Data) {
		w.runNodeServer(task.Data)
	} else if rePython.MatchString(task.Data) {
		w.executePythonCommand(task)
	} else if task.Data == "File" {
		fmt.Printf("Caught File Run request")
	} else {
		w.executeBashCommand(task)
	}
	log.Printf("Worker : %s processed task %s", string(w.id), task.TaskId)
}

func (w *WorkerService) SubmitFile(stream psm.WorkerService_SubmitFileServer) error {
	log.Printf("Worker : %s started receiving file tasks", string(w.id))

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			log.Printf("Worker : %s finished receiving file tasks", string(w.id))
			break
		}
		if err != nil {
			log.Printf("Worker : %s error receiving file task: %v", string(w.id), err)
			return err
		}

		go w.updateFileStatus(req, psm.TaskStatus_STARTED)
		log.Printf("Worker : %s received file task %s", string(w.id), req.TaskId)

		fileName, err := w.saveFile(req.TaskId, req.FileBuffer)
		if err != nil {
			log.Printf("Worker : %s failed to save file for task %s: %v", string(w.id), req.TaskId, err)
			return err
		}

		w.processFile(fileName, req.TaskId)

		go w.updateFileStatus(req, psm.TaskStatus_COMPLETED)
	}

	return stream.SendAndClose(&psm.SubmitFileResponse{
		Message: "File tasks were submitted successfully",
		Success: true,
	})
}

func (w *WorkerService) saveFile(taskID string, fileBuffer []byte) (string, error) {
	fileName := fmt.Sprintf("/tmp/task-%s.py", taskID)
	err := os.WriteFile(fileName, fileBuffer, 0644)
	if err != nil {
		log.Printf("Worker : %s failed to save file for task %s: %v", string(w.id), taskID, err)
		return "", err
	}

	return fileName, nil
}

func (w *WorkerService) processFile(fileName string, taskID string) {
	log.Printf("Worker : %s processing file task %s", string(w.id), taskID)

	defer func() {
		err := os.Remove(fileName)
		if err != nil {
			log.Printf("Worker : %s failed to delete file %s: %v", string(w.id), fileName, err)
		} else {
			log.Printf("Worker : %s successfully deleted file %s", string(w.id), fileName)
		}
	}()

	cmd := exec.Command("python3", fileName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("Worker : %s failed to execute file for task %s: %v", string(w.id), taskID, err)
	} else {
		log.Printf("Worker : %s successfully processed file task %s", string(w.id), taskID)
	}

	w.updateStatus(&psm.SubmitTaskRequest{TaskId: taskID}, psm.TaskStatus_COMPLETED)
}

// Execute Scripts

func (w *WorkerService) executePythonCommand(task *psm.SubmitTaskRequest) {
	log.Printf("Worker : %s executing Python command for task %s", string(w.id), task.TaskId)
	log.Printf("Full task data: %s", task.Data)

	pythonScript := strings.TrimPrefix(task.Data, "python3 -c ")
	pythonScript = pythonScript[1 : len(pythonScript)-1]
	pythonScript = strings.ReplaceAll(pythonScript, `\"`, `"`)

	log.Printf("Python script to execute:\n%s", pythonScript)

	// Create a temporary file
	tmpfile, err := os.CreateTemp("", "python-script-*.py")
	if err != nil {
		log.Printf("Worker : %s failed to create temporary file: %v", string(w.id), err)
		return
	}
	defer os.Remove(tmpfile.Name()) // Clean up

	// Write the Python script to the temporary file
	if _, err := tmpfile.WriteString(pythonScript); err != nil {
		log.Printf("Worker : %s failed to write to temporary file: %v", string(w.id), err)
		return
	}
	if err := tmpfile.Close(); err != nil {
		log.Printf("Worker : %s failed to close temporary file: %v", string(w.id), err)
		return
	}

	// Execute the Python script
	cmd := exec.Command("python3", tmpfile.Name())
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Worker : %s failed to execute Python script for task %s: %v\nOutput: %s", string(w.id), task.TaskId, err, string(output))
		return
	}

	log.Printf("Worker : %s successfully executed Python script for task %s", string(w.id), task.TaskId)
	log.Printf("Python script output:\n%s", string(output))
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

func (w *WorkerService) runNodeServer(command string) {
	cmd := exec.Command("bash", "-c", command)

	// Direct the command's output to the standard output and error streams
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Run the command
	err := cmd.Run()
	if err != nil {
		log.Printf("Worker : %s failed to execute command: %v", string(w.id), err)
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

func (w *WorkerService) updateFileStatus(task *psm.SubmitFileRequest, status psm.TaskStatus) {
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
