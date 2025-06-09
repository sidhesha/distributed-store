# SurfStore - Distributed File System

A distributed file system implementation using gRPC and Raft consensus protocol for fault tolerance and consistency.

## Features

- **Distributed Storage**: Block-based file storage across multiple servers
- **Raft Consensus**: Leader election and log replication for fault tolerance
- **Consistent Hashing**: Efficient data distribution and retrieval
- **gRPC Communication**: High-performance RPC framework for client-server interaction
- **Metadata Management**: Centralized file metadata with distributed consensus

## Architecture

The system consists of three main components:

- **BlockStore**: Handles storage and retrieval of file blocks
- **MetaStore**: Manages file metadata using Raft consensus
- **Client**: Provides interface for file operations

## Prerequisites

- Go 1.19 or higher
- Protocol Buffers compiler (`protoc`)

## Setup

### 1. Install Protocol Buffer Plugins

Install the required Go plugins for protocol buffer compilation:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
```

### 2. Update PATH

Ensure the protocol compiler can find the plugins:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

### 3. Generate Protocol Buffer Code

Generate Go code from the protocol buffer definitions:

```bash
protoc --proto_path=. --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       pkg/surfstore/SurfStore.proto
```

## Usage

### Starting the Servers

Start the BlockStore server:
```bash
make run-blockstore
```

Start a RaftSurfstore server (replace IDX with server index):
```bash
make IDX=0 run-raft
```

### Running Tests

Run all tests:
```bash
make test
```

Run specific tests:
```bash
make TEST_REGEX=TestName specific-test
```

### Cleanup

Clean build artifacts:
```bash
make clean
```

## Implementation Details

### Raft Integration

The MetaStore functionality is implemented using the Raft consensus protocol through `RaftSurfstoreServer`. This ensures:

- **Leader Election**: Automatic leader selection among replica servers
- **Log Replication**: Consistent state across all replicas
- **Fault Tolerance**: System continues operating despite server failures

### Client Configuration

The client connects to RaftSurfstore servers instead of direct MetaStore connections:

```go
c := NewRaftSurfstoreClient(conn)
conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
```

## File Structure

```
├── pkg/surfstore/
│   ├── SurfStore.proto      # Protocol buffer definitions
│   ├── Blockstore.go        # Block storage implementation
│   ├── Metastore.go         # Metadata management
│   ├── ConsistentHashRing.go # Hash ring for data distribution
│   ├── SurfstoreHelper.go   # Utility functions
│   └── SurfstoreUtils.go    # Additional utilities
├── cmd/SurfstoreServerExec/
│   └── main.go              # Server entry point
└── Makefile                 # Build and run commands
```

## Contributing

1. Follow Go coding standards
2. Ensure all tests pass before submitting changes
3. Update documentation for any API changes

## License

This project is part of a distributed systems implementation study.
