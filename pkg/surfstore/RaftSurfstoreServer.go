package surfstore

import (
	context "context"
	"log"
	"strings"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	serverStatus      ServerStatus
	serverStatusMutex *sync.RWMutex
	term              int64
	log               []*UpdateOperation
	id                int64
	metaStore         *MetaStore
	commitIndex       int64

	raftStateMutex *sync.RWMutex

	rpcConns   []*grpc.ClientConn
	grpcServer *grpc.Server

	peers       []string
	lastApplied int64
	matchIndex  []int64
	nextIndex   []int64
	/*--------------- Chaos Monkey --------------*/
	unreachableFrom map[int64]bool
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Ensure that the majority of servers are up
	log.Println("called GetFileInfoMap", s.id)
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()
	if serverStatus == ServerStatus_CRASHED {
		return &FileInfoMap{}, ErrServerCrashed
	}
	if serverStatus == ServerStatus_FOLLOWER {
		return &FileInfoMap{}, ErrNotLeader
	}
	_, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if err != nil {
		return &FileInfoMap{}, err
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Ensure that the majority of servers are up
	log.Println("called GetBlockStoreMap", s.id)
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()
	if serverStatus == ServerStatus_CRASHED {
		return &BlockStoreMap{}, ErrServerCrashed
	}
	if serverStatus == ServerStatus_FOLLOWER {
		return &BlockStoreMap{}, ErrNotLeader
	}
	_, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if err != nil {
		return &BlockStoreMap{}, err
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)

}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Ensure that the majority of servers are up
	log.Println("called GetBlockStoreAddrs", s.id)
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()
	if serverStatus == ServerStatus_CRASHED {
		return &BlockStoreAddrs{}, ErrServerCrashed
	}
	if serverStatus == ServerStatus_FOLLOWER {
		return &BlockStoreAddrs{}, ErrNotLeader
	}
	_, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if err != nil {
		return &BlockStoreAddrs{}, err
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)

}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// Ensure that the request gets replicated on majority of the servers.
	// Commit the entries and then apply to the state machine
	// cehck if leader. if not then success false return error
	log.Println("In update file for", s.id)
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	curr_term := s.term
	s.serverStatusMutex.RUnlock()
	if serverStatus == ServerStatus_CRASHED {
		log.Println("This is crashed", s.id)
		return &Version{Version: -1}, ErrServerCrashed
	}
	if serverStatus == ServerStatus_FOLLOWER {
		log.Println("This is follower", s.id)
		return &Version{Version: -1}, ErrNotLeader
	}
	// add to log.
	to_add := UpdateOperation{
		Term:         curr_term,
		FileMetaData: filemeta,
	}
	s.raftStateMutex.Lock()
	s.log = append(s.log, &to_add)
	log.Println("Added to log for", s.id, "log:", s.log)
	s.raftStateMutex.Unlock()
	// sendheartbeat
	_, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	log.Println("After sendhearbead", s.id, "log", s.log, "nexindex:", s.nextIndex, "matchedindx:", s.matchIndex)
	// cehck if leader. if not then version -1 return error
	if err != nil {
		log.Println("Got error from sendheartbeat", s.id, err)
		return &Version{Version: -1}, err
	}
	s.serverStatusMutex.Lock()
	// s.commitIndex = curr_best_option
	s.commitIndex = int64(len(s.log) - 1)
	s.serverStatusMutex.Unlock()
	to_return := Version{Version: -1}
	for {
		log.Println("in for loop commit part", "lastappplied for", s.id, "is", s.lastApplied, "with commitindex", s.commitIndex)
		to_break := false
		s.raftStateMutex.Lock()
		if s.lastApplied >= s.commitIndex {
			s.raftStateMutex.Unlock()
			break
		}
		if s.lastApplied == s.commitIndex-1 {
			to_break = true
		}
		log.Println("lastappplied for", s.id, "is", s.lastApplied, "with commitindex", s.commitIndex)
		to_commit := s.log[s.lastApplied+1]
		s.raftStateMutex.Unlock()
		if to_commit.FileMetaData != nil {
			returned_version, _ := s.metaStore.UpdateFile(ctx, to_commit.FileMetaData)
			// if err != nil {
			// 	return nil, err
			// }
			to_return.Version = returned_version.Version
		}
		s.raftStateMutex.Lock()
		s.lastApplied = s.lastApplied + 1
		s.raftStateMutex.Unlock()
		if to_break {
			break
		}
	}
	// log.Println("Returning for", s.id, "file version", &to_return)
	return &to_return, nil
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex or whose term
// doesn't match prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	log.Println("in appendentries for", s.id)
	defer log.Println("returning from append entries", s.id)
	s.serverStatusMutex.RLock()
	curr_term := s.term
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()
	s.raftStateMutex.Lock()
	if s.unreachableFrom[input.LeaderId] {
		crash_output := AppendEntryOutput{
			ServerId:     s.id,
			Term:         s.term,
			Success:      false,
			MatchedIndex: s.lastApplied,
		}
		s.raftStateMutex.Unlock()
		log.Println("retruning from unreachable", s.id, crash_output.ServerId, &crash_output)
		return &crash_output, ErrServerCrashedUnreachable
	}
	s.raftStateMutex.Unlock()
	log.Println("serveer status for", s.id, serverStatus == ServerStatus_CRASHED)
	if serverStatus == ServerStatus_CRASHED {
		s.raftStateMutex.Lock()
		crash_output := AppendEntryOutput{
			ServerId:     s.id,
			Term:         s.term,
			Success:      false,
			MatchedIndex: s.lastApplied,
		}
		s.raftStateMutex.Unlock()
		log.Println("retruning from crashed", s.id, crash_output.ServerId, &crash_output)
		return &crash_output, ErrServerCrashedUnreachable
		// return nil, fmt.Errorf("custom error crahsed")
	}
	// 1st part
	// leader term < peer term
	log.Println("beforer 1st part", s.id)
	if input.Term < curr_term {
		// mytodo: dont know how to handle this tho
		s.raftStateMutex.Lock()
		temp_output := &AppendEntryOutput{
			Success:      false,
			ServerId:     s.id,
			Term:         s.term,
			MatchedIndex: s.lastApplied,
		}
		s.raftStateMutex.Unlock()
		return temp_output, nil
	}
	// 1.5th part
	log.Println("beforer 1.5th part", s.id)
	if input.Term > curr_term {
		s.raftStateMutex.Lock()
		s.term = input.Term
		s.serverStatus = ServerStatus_FOLLOWER
		curr_term = input.Term
		s.raftStateMutex.Unlock()
	}
	// 2nd part
	log.Println("beforer 2nd part", s.id)
	s.raftStateMutex.Lock()
	if (input.PrevLogIndex >= 0 && int64(len(s.log)) > input.PrevLogIndex && s.log[input.PrevLogIndex].Term != input.PrevLogTerm) || (int64(len(s.log)) <= input.PrevLogIndex) {
		temp_output := AppendEntryOutput{
			ServerId:     s.id,
			Success:      false,
			Term:         s.term,
			MatchedIndex: s.lastApplied,
		}
		s.raftStateMutex.Unlock()
		// mytodo: returning false but what error will have to check
		return &temp_output, ErrPlzSendOlder
	}
	// 3rd part
	log.Println("beforer 3rd part", s.id, "input leader commit", input.LeaderCommit)
	// i assume that PrevLogIndex exists in log.
	s.log = s.log[:input.PrevLogIndex+1]
	// 4th part
	s.log = append(s.log, input.Entries...)
	// 5th part
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = min(input.LeaderCommit, int64(len(s.log)-1))
	}
	s.raftStateMutex.Unlock()
	// commit time.
	log.Println("beforer for loop commit part", s.id)
	for {
		log.Println("in for loop commit part", "lastappplied for", s.id, "is", s.lastApplied, "with commitindex", s.commitIndex)
		to_break := false
		s.raftStateMutex.Lock()
		if s.lastApplied >= s.commitIndex {
			s.raftStateMutex.Unlock()
			break
		}
		if s.lastApplied == s.commitIndex-1 {
			to_break = true
		}
		log.Println("lastappplied for", s.id, "is", s.lastApplied, "with commitindex", s.commitIndex)
		to_commit := s.log[s.lastApplied+1]
		s.raftStateMutex.Unlock()
		if to_commit.FileMetaData != nil {
			s.metaStore.UpdateFile(ctx, to_commit.FileMetaData)
			// if err != nil {
			// 	return nil, err
			// }
		}
		s.raftStateMutex.Lock()
		s.lastApplied = s.lastApplied + 1
		s.raftStateMutex.Unlock()
		if to_break {
			break
		}
	}
	// everything successful if here
	s.raftStateMutex.Lock()
	to_return := AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      true,
		MatchedIndex: s.lastApplied,
	}
	s.raftStateMutex.Unlock()
	return &to_return, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()
	if serverStatus == ServerStatus_CRASHED {
		return &Success{Flag: false}, ErrServerCrashed
	}
	// update term and status and noop
	s.raftStateMutex.Lock()
	s.term = s.term + 1
	s.serverStatus = ServerStatus_LEADER
	no_op := UpdateOperation{
		Term:         s.term,
		FileMetaData: nil,
	}
	// update matchindex nextindex
	for curr_peer_id, _ := range s.peers {
		if s.id == int64(curr_peer_id) {
			// s.nextIndex[curr_peer_id] = int64(len(s.log))
			// s.matchIndex[curr_peer_id] = int64(len(s.log))
			continue
		}
		s.nextIndex[curr_peer_id] = int64(len(s.log))
		s.matchIndex[curr_peer_id] = int64(-1)
	}
	s.raftStateMutex.Unlock()
	// noop udpatefile
	_, err := s.UpdateFile(ctx, no_op.FileMetaData)
	if err != nil {
		// if(recv_ver.Version == -1){
		// 	return &Success{Flag: false}, err
		// }
		log.Println("error in no_op updatefile", err)
		return &Success{Flag: false}, err
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")
	// check leader crash
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	curr_peers := s.peers
	curr_id := s.id
	// curr_term := s.term
	s.serverStatusMutex.RUnlock()
	if serverStatus == ServerStatus_CRASHED {
		return &Success{Flag: false}, ErrServerCrashed
	}
	if serverStatus == ServerStatus_FOLLOWER {
		return &Success{Flag: false}, ErrNotLeader
	}
	keep_retrying := true
	for keep_retrying {
		log.Println("in sendheartbeat loop", s.id)
		type append_output struct {
			out *AppendEntryOutput
			err error
		}
		output_channel := make(chan append_output)
		for curr_peer_id, _ := range curr_peers {
			if curr_id == int64(curr_peer_id) {
				continue
			}
			s.raftStateMutex.RLock()
			curr_peer := NewRaftSurfstoreClient(s.rpcConns[curr_peer_id])
			s.raftStateMutex.RUnlock()
			s.raftStateMutex.RLock()
			temp_prev_log_term := int64(-1)
			if s.nextIndex[curr_peer_id]-1 >= 0 {
				temp_prev_log_term = s.log[s.nextIndex[curr_peer_id]-1].Term
			}
			to_send := &AppendEntryInput{
				Term:         s.term,
				LeaderId:     curr_id,
				PrevLogIndex: s.nextIndex[curr_peer_id] - 1,
				PrevLogTerm:  temp_prev_log_term,
				LeaderCommit: s.commitIndex,
				Entries:      s.log[s.nextIndex[curr_peer_id]:],
			}
			s.raftStateMutex.RUnlock()
			go func(output_channel chan append_output, sending *AppendEntryInput, curr_peer RaftSurfstoreClient, curr_peer_id int) {
				x, err := curr_peer.AppendEntries(ctx, sending)
				var output_append *AppendEntryOutput = x
				log.Println("x is recvd", x)
				if output_append == nil && err != nil {
					output_append = &AppendEntryOutput{
						Success:  false,
						Term:     0,
						ServerId: int64(curr_peer_id),
					}
					log.Println("output append", output_append)
				}

				log.Println("recvd append entry", output_append, err)
				temp_append_output := append_output{out: output_append, err: err}
				output_channel <- temp_append_output
			}(output_channel, to_send, curr_peer, curr_peer_id)

		}
		// actual_num_peers := len(curr_peers) - 1
		is_peer_online := make([]bool, len(curr_peers))
		peer_append_successful := make([]bool, len(curr_peers))
		is_majority_up := false
		for id, _ := range is_peer_online {
			is_peer_online[id] = true
			if id == int(curr_id) {
				peer_append_successful[id] = true
			} else {
				peer_append_successful[id] = false
			}
		}
		count := 0
		for {
			temp_append_output := <-output_channel
			count += 1
			log.Println("got output", temp_append_output.out, temp_append_output.err)
			// mytodo: need to recheck break logic

			actual_output := temp_append_output.out
			err := temp_append_output.err
			if err != nil {
				log.Println("got err: ", err.Error())
				// mytodo: currently 2 type of error from AppendEntries. and one type of failure
				// 1st case ErrServerCrashedUnreachable
				if strings.Contains(err.Error(), "crashed") {
					// do nothing
					is_peer_online[actual_output.ServerId] = false
					peer_append_successful[actual_output.ServerId] = false
					log.Println("is perr online", is_peer_online)
				} else if strings.Contains(err.Error(), "older") {
					// 2nd  when previndex
					log.Println("----------------reached asking older log for:", actual_output.ServerId)
					s.raftStateMutex.Lock()
					s.nextIndex[actual_output.ServerId] = max(0, s.nextIndex[actual_output.ServerId]-1)
					// as per rppc thing i dont think it will get used
					// s.matchIndex[actual_output.ServerId] = actual_output.MatchedIndex
					s.raftStateMutex.Unlock()
					is_peer_online[actual_output.ServerId] = true
					peer_append_successful[actual_output.ServerId] = false
					// continue
				}
			} else if err == nil && !actual_output.Success {
				s.raftStateMutex.Lock()
				s.serverStatus = ServerStatus_FOLLOWER
				s.term = actual_output.Term
				s.raftStateMutex.Unlock()
				return &Success{Flag: false}, ErrNotLeader
			} else if err == nil && actual_output.Success {
				// nextindex hadnle
				is_peer_online[actual_output.ServerId] = true
				peer_append_successful[actual_output.ServerId] = true
				s.raftStateMutex.Lock()
				s.nextIndex[actual_output.ServerId] = int64(len(s.log))
				s.matchIndex[actual_output.ServerId] = actual_output.MatchedIndex
				s.raftStateMutex.Unlock()
			}
			// check majority
			total_succ := 0
			for _, succ := range peer_append_successful {
				if succ {
					total_succ += 1
				}
			}
			if total_succ > len(peer_append_successful)/2 {
				is_majority_up = true
			}
			if count == len(s.peers)-1 {
				break
			}
		}
		// if majority up and every online's append true then retry = false
		should_end := true
		for id, _ := range is_peer_online {
			if is_peer_online[id] && !peer_append_successful[id] {
				should_end = false
			}
		}
		log.Println("chceking retrying", is_majority_up, should_end, is_peer_online, peer_append_successful)
		if should_end && is_majority_up {
			keep_retrying = false
		}
		// reducing sleep
		time.Sleep(175 * time.Millisecond)
	}
	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) MakeServerUnreachableFrom(ctx context.Context, servers *UnreachableFromServers) (*Success, error) {
	s.raftStateMutex.Lock()
	if len(servers.ServerIds) == 0 {
		s.unreachableFrom = make(map[int64]bool)
		log.Printf("Server %d is reachable from all servers", s.id)
	} else {
		for _, serverId := range servers.ServerIds {
			s.unreachableFrom[serverId] = true
		}
		log.Printf("Server %d is unreachable from %v", s.id, s.unreachableFrom)
	}

	s.raftStateMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_CRASHED
	log.Printf("Server %d is crashed", s.id)
	s.serverStatusMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_FOLLOWER
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.unreachableFrom = make(map[int64]bool)
	s.raftStateMutex.Unlock()

	log.Printf("Server %d is restored to follower and reachable from all servers", s.id)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	// fmt.Println("started GetInternalState for", s.id)
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	// fmt.Println("got fileInfoMap for", s.id)
	s.serverStatusMutex.RLock()
	s.raftStateMutex.RLock()
	state := &RaftInternalState{
		Status:      s.serverStatus,
		Term:        s.term,
		CommitIndex: s.commitIndex,
		Log:         s.log,
		MetaMap:     fileInfoMap,
	}
	s.raftStateMutex.RUnlock()
	s.serverStatusMutex.RUnlock()
	// fmt.Println("got state for", s.id, ": ", state)
	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
