# Distributed Task Scheduler

A robust distributed task scheduling system built with Go, gRPC, and PostgreSQL, designed to efficiently manage and execute distributed tasks across multiple workers.

## Architecture

```mermaid
flowchart TB
    subgraph Client["Client Applications"]
        C1[Client 1]
        C2[Client 2]
        C3[Client N]
    end

    subgraph Core["Core Components"]
        S[Scheduler]
        CO[Coordinator]
        DB[(PostgreSQL DB)]
    end

    subgraph Workers["Worker Pool"]
        W1[Worker 1]
        W2[Worker 2]
        W3[Worker N]
    end

    subgraph Tasks["Worker Tasks"]
        T1[HTTP Server 1]
        T2[HTTP Server 2]
        T3[HTTP Server N]
    end

    C1 & C2 & C3 --> S
    S --> DB
    CO --> DB
    CO <--> W1 & W2 & W3
    W1 --> T1
    W2 --> T2
    W3 --> T3
```

## System Components

### Scheduler

- Handles task scheduling and management
- Stores task definitions and schedules in PostgreSQL
- Provides API for task submission and management

### Coordinator

- Manages worker pool and task distribution
- Performs periodic database scans for pending tasks
- Implements Round-Robin task assignment algorithm
- Monitors worker health and status through regular heartbeat.
- Handles worker failover and task reassignment

### Workers

- Execute assigned tasks independently
- Run HTTP servers for task-specific operations
- Implement health check endpoints
- Maintain persistent connection with coordinator
- Support automatic restart mechanisms

### PostgreSQL Database

- Stores task definitions and schedules
- Maintains worker and task state
- Ensures data persistence and reliability
- Handles concurrent access efficiently

## Features

- **Distributed Task Execution**: Efficiently distribute tasks across multiple workers
- **Health Monitoring**: Comprehensive health check system
  - Worker connectivity status
  - Task processing state
  - Database connection health
  - Server availability checks
- **Automatic Recovery**: Self-healing mechanisms for worker failures
- **Scalable Architecture**: Easy to scale by adding more workers
- **Docker Support**: Containerized deployment with Docker Compose
- **gRPC Communication**: High-performance inter-service communication

## Getting Started

### Prerequisites

- Go 1.19 or higher
- PostgreSQL 13 or higher
- Docker and Docker Compose
- Prisma CLI

### Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/distributed-task-scheduler.git
cd distributed-task-scheduler
```

3. Start the services:

```bash
docker-compose build
docker-compose up --scale worker=3
```

This will start 3 worker images.

### Configuration

Key configuration options in `.env`:

```env
DATABASE_URL="postgresql://postgres:admin@postgres:5432/postgres?schema=public"

COORDINATOR_ADDRESS="http://localhost:8081"
```

### Worker Health Checks

Each worker implements a comprehensive health check system:

```go
// Example health check endpoint
func (w *Worker) HealthCheck() *HealthStatus {
    return &HealthStatus{
        CoordinatorConnection: w.checkCoordinatorConnection(),
        TaskProcessingStatus: w.getTaskProcessingStatus(),
        DatabaseConnection: w.checkDatabaseConnection(),
        ServerStatus: w.checkServerStatus(),
    }
}
```
