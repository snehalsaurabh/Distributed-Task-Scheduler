package main

import (
	"context"
	"dts/psm"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
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
	// CreateTask2(laptopClient)
	// CreateCurlTask(laptopClient)
	// CreateTask(laptopClient)
	// CreateTask(laptopClient)
	// CreateTask(laptopClient)
	// CreatePythonTask(laptopClient)
	// CreatePythonTask2(laptopClient)
	TransferFile(laptopClient, "assets/index.py")

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

func CreateCurlTask(laptopClient psm.ClientServiceClient) {

	req := &psm.ScheduleTaskRequest{
		Command:     `curl -X GET "https://jsonplaceholder.typicode.com/todos/1"`,
		ScheduledAt: time.Now().Format(time.RFC3339),
	}

	res, err := laptopClient.ScheduleTask(context.Background(), req)
	if err != nil {
		panic(err)
	}

	fmt.Println("Task created\n", res)
}

func CreatePythonTask(laptopClient psm.ClientServiceClient) {
	pythonScript := `
# Function to reverse a string
def reverse_string(s):
    return s[::-1]

# Function to check if a string is a palindrome
def is_palindrome(s):
    reversed_s = reverse_string(s)
    return s == reversed_s

# Generate some random numbers without using libraries
numbers = [i for i in range(1, 11)]  # Just for illustration
print(f"Original numbers: {numbers}")

# Sort the numbers
sorted_numbers = sorted(numbers, reverse=True)
print(f"Sorted numbers (descending): {sorted_numbers}")

# String manipulation
test_string = "madam"
print(f"Original string: {test_string}")
reversed_string = reverse_string(test_string)
print(f"Reversed string: {reversed_string}")
print(f"Is '{test_string}' a palindrome? {'Yes' if is_palindrome(test_string) else 'No'}")

# Perform a simple calculation
sum_of_numbers = sum(numbers)
print(f"Sum of numbers: {sum_of_numbers}")

average = sum_of_numbers / len(numbers)
print(f"Average of numbers: {average}")

# Find max and min
print(f"Maximum number: {max(numbers)}")
print(f"Minimum number: {min(numbers)}")
`

	// Escape only the double quotes in the Python script
	escapedScript := strings.ReplaceAll(pythonScript, `"`, `\"`)

	// Prepend the Python interpreter command and wrap the script in double quotes
	command := fmt.Sprintf(`python3 -c "%s"`, escapedScript)

	req := &psm.ScheduleTaskRequest{
		Command:     command,
		ScheduledAt: time.Now().Format(time.RFC3339),
	}

	res, err := laptopClient.ScheduleTask(context.Background(), req)
	if err != nil {
		log.Fatalf("Failed to schedule Python task: %v", err)
	}

	fmt.Printf("Python task scheduled: %+v\n", res)
	fmt.Printf("Scheduled command: %s\n", command)
}

func CreatePythonTask2(laptopClient psm.ClientServiceClient) {
	pythonScript := `
import time

# A long-running loop that takes 3-4 minutes
print("Starting a time-consuming task...")

for i in range(1, 1000000):  # 100 iterations
    print(f"Iteration {i}/1000000 completed")

print("Task completed after approximately 3 minutes and 20 seconds.")
`

	// Escape only the double quotes in the Python script
	escapedScript := strings.ReplaceAll(pythonScript, `"`, `\"`)

	// Prepend the Python interpreter command and wrap the script in double quotes
	command := fmt.Sprintf(`python3 -c "%s"`, escapedScript)

	req := &psm.ScheduleTaskRequest{
		Command:     command,
		ScheduledAt: time.Now().Format(time.RFC3339),
	}

	res, err := laptopClient.ScheduleTask(context.Background(), req)
	if err != nil {
		log.Fatalf("Failed to schedule Python task: %v", err)
	}

	fmt.Printf("Python task scheduled: %+v\n", res)
	fmt.Printf("Scheduled command: %s\n", command)
}

func TransferFile(laptopClient psm.ClientServiceClient, filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()
	log.Printf("Unimplemented here")
	stream, err := laptopClient.TransferFile(context.Background())
	if err != nil {
		log.Fatalf("Failed to create file transfer stream: %v", err)
	}

	const chunkSize = 1024
	buffer := make([]byte, chunkSize)

	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error reading file: %v", err)
		}

		chunk := &psm.FileChunk{
			Filename: filepath.Base(filePath),
			Content:  buffer[:n],
			IsLast:   false,
		}

		if err := stream.Send(chunk); err != nil {
			log.Fatalf("Failed to send file chunk: %v", err)
		}
	}

	// Send the last chunk
	lastChunk := &psm.FileChunk{
		Filename: filepath.Base(filePath),
		Content:  []byte{},
		IsLast:   true,
	}
	if err := stream.Send(lastChunk); err != nil {
		log.Fatalf("Failed to send last chunk: %v", err)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Failed to receive response: %v", err)
	}

	fmt.Printf("File transfer response: Success=%v, Message=%s, FileId=%s\n", res.Success, res.Message, res.FileId)
}
