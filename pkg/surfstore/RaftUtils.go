package surfstore

import (
	"bufio"
	// context "context"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here
	conns := make([]*grpc.ClientConn, 0)
	for _, addr := range config.RaftAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}

	serverStatusMutex := sync.RWMutex{}
	raftStateMutex := sync.RWMutex{}

	server := RaftSurfstore{
		serverStatus:      ServerStatus_FOLLOWER,
		serverStatusMutex: &serverStatusMutex,
		term:              0,
		metaStore:         NewMetaStore(config.BlockAddrs),
		log:               make([]*UpdateOperation, 0),

		id:          id,
		commitIndex: -1,

		unreachableFrom: make(map[int64]bool),
		grpcServer:      grpc.NewServer(),
		rpcConns:        conns,

		raftStateMutex: &raftStateMutex,

		peers:       config.RaftAddrs,
		lastApplied: -1,
		matchIndex:  make([]int64, len(config.RaftAddrs)),
		nextIndex:   make([]int64, len(config.RaftAddrs)),
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	RegisterRaftSurfstoreServer(server.grpcServer, server)
	listener, err := net.Listen("tcp", server.peers[server.id])
	if err != nil {
		log.Println("error starting raft server:", server.peers[server.id], err)
		return err
	}
	return server.grpcServer.Serve(listener)
}

func (s *RaftSurfstore) checkStatus() error {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return ErrServerCrashed
	}

	if serverStatus != ServerStatus_LEADER {
		return ErrNotLeader
	}

	return nil
}
