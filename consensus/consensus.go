package consensus

import (
	"errors"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"

	db "github.com/abhishek-agrl/transaction-db"
)

// NodeState defines the cluster roles a node can assume (Leader, Follower, Candidate).
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

var (
	ErrNotLeader         = errors.New("consensus: node is not the leader")
	ErrReplicationFailed = errors.New("consensus: quorum write replication failed")
)

// String returns a human-readable representation of NodeState.
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RequestVoteArgs carries RPC election vote parameters.
type RequestVoteArgs struct {
	Term        int
	CandidateID string
}

// RequestVoteReply holds the election vote response.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// ReplicateDataArgs carries replication payloads or heartbeats.
type ReplicateDataArgs struct {
	Term      int
	LeaderID  string
	Keys      [][]byte
	Values    [][]byte
	IsDeleted []bool
}

// ReplicateDataReply holds the replication outcome.
type ReplicateDataReply struct {
	Term    int
	Success bool
}

// Node RPC receiver wrapper to avoid exposing raw Node fields to net/rpc registry.
type NodeRPC struct {
	node *Node
}

// RequestVote handles inter-node vote solicitations.
func (r *NodeRPC) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	return r.node.HandleRequestVote(args, reply)
}

// ReplicateData handles database replication payload streaming and heartbeat resets.
func (r *NodeRPC) ReplicateData(args *ReplicateDataArgs, reply *ReplicateDataReply) error {
	return r.node.HandleReplicateData(args, reply)
}

// Node represents a single member node in the distributed replication group.
type Node struct {
	mu            sync.Mutex
	id            string
	rpcAddr       string
	peers         []string // Peer addresses (excludes own address)
	db            db.TransactionDB
	state         NodeState
	currentTerm   int
	votedFor      string
	rpcServer     *rpc.Server
	listener      net.Listener
	closed        bool
	lastHeartbeat time.Time
	rng           *rand.Rand
	wg            sync.WaitGroup
	stopChan      chan struct{}
}

// NewNode initializes a new consensus node mapping to the given storage backend.
func NewNode(id string, rpcAddr string, peers []string, database db.TransactionDB) *Node {
	return &Node{
		id:            id,
		rpcAddr:       rpcAddr,
		peers:         peers,
		db:            database,
		state:         Follower,
		lastHeartbeat: time.Now(),
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
		stopChan:      make(chan struct{}),
	}
}

// ID returns the node identifier.
func (n *Node) ID() string {
	return n.id
}

// State returns the current cluster state.
func (n *Node) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// Term returns the current term of the node.
func (n *Node) Term() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// Start launches the RPC Server and consensus routines.
func (n *Node) Start() error {
	n.mu.Lock()
	n.closed = false
	n.lastHeartbeat = time.Now()
	n.mu.Unlock()

	// 1. Establish custom RPC Server to prevent collisions in unit testing
	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(&NodeRPC{node: n}); err != nil {
		return err
	}
	n.rpcServer = rpcServer

	l, err := net.Listen("tcp", n.rpcAddr)
	if err != nil {
		return err
	}
	n.listener = l

	// Serve incoming RPC requests
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				n.mu.Lock()
				closed := n.closed
				n.mu.Unlock()
				if closed {
					return
				}
				continue
			}
			go rpcServer.ServeConn(conn)
		}
	}()

	// 2. Start heartbeat starvation detection loop
	n.wg.Add(1)
	go n.electionLoop()

	// 3. Start heartbeat emission loop (for when node assumes Leader state)
	n.wg.Add(1)
	go n.heartbeatLoop()

	return nil
}

// getRandomTimeout generates a randomized timeout between 150ms and 300ms.
func (n *Node) getRandomTimeout() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	// 150ms base + rand [0, 150)ms
	return time.Duration(150+n.rng.Intn(150)) * time.Millisecond
}

// electionLoop monitors heartbeat timeouts and triggers elections when starved.
func (n *Node) electionLoop() {
	defer n.wg.Done()

	for {
		timeout := n.getRandomTimeout()
		select {
		case <-n.stopChan:
			return
		case <-time.After(timeout):
			n.mu.Lock()
			if n.state == Leader {
				n.mu.Unlock()
				continue
			}

			if time.Since(n.lastHeartbeat) >= timeout {
				// Heartbeat timed out: trigger leader election
				n.startElection()
			}
			n.mu.Unlock()
		}
	}
}

// startElection solicits votes from peers to claim leadership.
func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.lastHeartbeat = time.Now()

	term := n.currentTerm
	candidateID := n.id
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)

	// Temporarily unlock state mutex to prevent deadlocks while blocking on network RPCs
	n.mu.Unlock()
	defer n.mu.Lock()

	votes := 1 // Vote for self
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			args := RequestVoteArgs{
				Term:        term,
				CandidateID: candidateID,
			}
			var reply RequestVoteReply

			client, err := rpc.Dial("tcp", peerAddr)
			if err != nil {
				return
			}
			defer client.Close()

			err = client.Call("NodeRPC.RequestVote", &args, &reply)
			if err != nil {
				return
			}

			n.mu.Lock()
			if reply.Term > n.currentTerm {
				// Peer has a higher term: step down to follower
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
				n.lastHeartbeat = time.Now()
			}
			n.mu.Unlock()

			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	n.mu.Lock()
	// Verify state and term remains stable before transition
	if n.state == Candidate && n.currentTerm == term {
		totalNodes := len(peers) + 1
		majority := (totalNodes / 2) + 1
		if votes >= majority {
			n.state = Leader
			go n.broadcastHeartbeats()
		}
	}
	n.mu.Unlock()
}

// heartbeatLoop emits pings periodically if the node is Leader.
func (n *Node) heartbeatLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopChan:
			return
		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.state == Leader
			n.mu.Unlock()
			if isLeader {
				n.broadcastHeartbeats()
			}
		}
	}
}

// broadcastHeartbeats sends replication heartbeats in parallel.
func (n *Node) broadcastHeartbeats() {
	n.mu.Lock()
	term := n.currentTerm
	leaderID := n.id
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	n.mu.Unlock()

	for _, peer := range peers {
		go func(peerAddr string) {
			args := ReplicateDataArgs{
				Term:     term,
				LeaderID: leaderID,
			}
			var reply ReplicateDataReply

			client, err := rpc.Dial("tcp", peerAddr)
			if err != nil {
				return
			}
			defer client.Close()

			err = client.Call("NodeRPC.ReplicateData", &args, &reply)
			if err != nil {
				return
			}

			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
				n.lastHeartbeat = time.Now()
			}
			n.mu.Unlock()
		}(peer)
	}
}

// HandleRequestVote executes election vote solicitation checks.
func (n *Node) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = ""
		n.lastHeartbeat = time.Now()
	}

	if args.Term == n.currentTerm && (n.votedFor == "" || n.votedFor == args.CandidateID) {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.lastHeartbeat = time.Now() // reset timeouts
	} else {
		reply.VoteGranted = false
	}

	reply.Term = n.currentTerm
	return nil
}

// HandleReplicateData executes heartbeat logs and replication mutations.
func (n *Node) HandleReplicateData(args *ReplicateDataArgs, reply *ReplicateDataReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = ""
		n.lastHeartbeat = time.Now()
	}

	if args.Term >= n.currentTerm {
		n.lastHeartbeat = time.Now()
		if n.state != Follower {
			n.state = Follower
			n.votedFor = ""
		}

		reply.Success = true

		// Write replication changes to local DB engine if present
		if len(args.Keys) > 0 {
			for i := range args.Keys {
				if args.IsDeleted[i] {
					_ = n.db.Delete(args.Keys[i])
				} else {
					_ = n.db.Put(args.Keys[i], args.Values[i])
				}
			}
		}
	} else {
		reply.Success = false
	}

	reply.Term = n.currentTerm
	return nil
}

// replicateWriteToPeers streams database writes to peers and evaluates quorum responses.
func (n *Node) replicateWriteToPeers(keys [][]byte, values [][]byte, isDeleted []bool) bool {
	n.mu.Lock()
	term := n.currentTerm
	leaderID := n.id
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	n.mu.Unlock()

	if len(peers) == 0 {
		return true // Quorum met on single-node cluster
	}

	successes := 1 // Self success
	var successMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			args := ReplicateDataArgs{
				Term:      term,
				LeaderID:  leaderID,
				Keys:      keys,
				Values:    values,
				IsDeleted: isDeleted,
			}
			var reply ReplicateDataReply

			client, err := rpc.Dial("tcp", peerAddr)
			if err != nil {
				return
			}
			defer client.Close()

			err = client.Call("NodeRPC.ReplicateData", &args, &reply)
			if err != nil {
				return
			}

			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
				n.lastHeartbeat = time.Now()
			}
			n.mu.Unlock()

			if reply.Success {
				successMu.Lock()
				successes++
				successMu.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	totalNodes := len(peers) + 1
	majority := (totalNodes / 2) + 1
	return successes >= majority
}

// Put routes a Put write request through write replication.
func (n *Node) Put(key db.Key, value db.Value) error {
	n.mu.Lock()
	isLeader := n.state == Leader
	n.mu.Unlock()

	if !isLeader {
		return ErrNotLeader
	}

	keys := [][]byte{key}
	values := [][]byte{value}
	isDeleted := []bool{false}

	if !n.replicateWriteToPeers(keys, values, isDeleted) {
		return ErrReplicationFailed
	}

	return n.db.Put(key, value)
}

// Delete routes a Delete write request through write replication.
func (n *Node) Delete(key db.Key) error {
	n.mu.Lock()
	isLeader := n.state == Leader
	n.mu.Unlock()

	if !isLeader {
		return ErrNotLeader
	}

	keys := [][]byte{key}
	values := [][]byte{nil}
	isDeleted := []bool{true}

	if !n.replicateWriteToPeers(keys, values, isDeleted) {
		return ErrReplicationFailed
	}

	return n.db.Delete(key)
}

// BatchPut routes a BatchPut write request through write replication.
func (n *Node) BatchPut(keys []db.Key, values []db.Value) error {
	n.mu.Lock()
	isLeader := n.state == Leader
	n.mu.Unlock()

	if !isLeader {
		return ErrNotLeader
	}

	keysBytes := make([][]byte, len(keys))
	valuesBytes := make([][]byte, len(values))
	isDeleted := make([]bool, len(keys))
	for i := range keys {
		keysBytes[i] = keys[i]
		valuesBytes[i] = values[i]
		isDeleted[i] = false
	}

	if !n.replicateWriteToPeers(keysBytes, valuesBytes, isDeleted) {
		return ErrReplicationFailed
	}

	return n.db.BatchPut(keys, values)
}

// Read retrieves a value from the node's local database.
func (n *Node) Read(key db.Key) (db.Value, error) {
	return n.db.Read(key)
}

// ReadKeyRange retrieves range scan records from the node's local database.
func (n *Node) ReadKeyRange(startKey, endKey db.Key) ([]db.Entry, error) {
	return n.db.ReadKeyRange(startKey, endKey)
}

// Stop shuts down the RPC listeners and halts election and heartbeat routines.
func (n *Node) Stop() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	close(n.stopChan)

	var err error
	if n.listener != nil {
		err = n.listener.Close()
	}
	n.mu.Unlock()

	n.wg.Wait()
	return err
}
