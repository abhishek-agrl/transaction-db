package tests

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	db "github.com/abhishek-agrl/transaction-db"
	"github.com/abhishek-agrl/transaction-db/consensus"
	"github.com/abhishek-agrl/transaction-db/storage"
)

// getFreeLocalAddr binds to an ephemeral port to find a free socket address, then closes it.
func getFreeLocalAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free local address: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func setupCluster(t *testing.T, tmpDir string) ([]*consensus.Node, []string) {
	nodeCount := 3
	addrs := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		addrs[i] = getFreeLocalAddr(t)
	}

	nodes := make([]*consensus.Node, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeDir := filepath.Join(tmpDir, fmt.Sprintf("node_%d", i))
		engine, err := storage.NewEngine(nodeDir)
		if err != nil {
			t.Fatalf("failed to create engine for node %d: %v", i, err)
		}

		// Configure peers list (excludes self address)
		peers := []string{}
		for j := 0; j < nodeCount; j++ {
			if i != j {
				peers = append(peers, addrs[j])
			}
		}

		nodes[i] = consensus.NewNode(fmt.Sprintf("node_%d", i), addrs[i], peers, engine)
	}

	return nodes, addrs
}

func TestConsensusElectionAndFailover(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "consensus_cluster_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	nodes, _ := setupCluster(t, tmpDir)

	// 1. Start all nodes in the cluster
	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("failed to start node %d: %v", i, err)
		}
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	// Wait for election to complete (ticker range is 150-300ms)
	time.Sleep(600 * time.Millisecond)

	// Assert that exactly one leader has been elected
	var leader *consensus.Node
	leaderCount := 0
	followers := []*consensus.Node{}

	for _, node := range nodes {
		if node.State() == consensus.Leader {
			leader = node
			leaderCount++
		} else if node.State() == consensus.Follower {
			followers = append(followers, node)
		}
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}
	if len(followers) != 2 {
		t.Fatalf("expected 2 followers, got %d", len(followers))
	}
	if leader.Term() == 0 {
		t.Error("expected leader term to be greater than 0")
	}

	// 2. Test Quorum Write Replication
	key := db.Key("replicatedKey")
	val := db.Value("consensusValue")

	// Writing to follower should fail
	err = followers[0].Put(key, val)
	if err != consensus.ErrNotLeader {
		t.Errorf("expected ErrNotLeader, got %v", err)
	}

	// Writing to leader should succeed (quorum = 2/3)
	err = leader.Put(key, val)
	if err != nil {
		t.Fatalf("failed to write to leader: %v", err)
	}

	// Verify local read on leader
	readVal, err := leader.Read(key)
	if err != nil {
		t.Errorf("failed to read from leader: %v", err)
	}
	if !bytes.Equal(readVal, val) {
		t.Errorf("leader value mismatch: expected %s, got %s", val, readVal)
	}

	// Verify replication on followers (allow a tiny window for network sync propagation)
	time.Sleep(100 * time.Millisecond)
	for _, f := range followers {
		fVal, err := f.Read(key)
		if err != nil {
			t.Errorf("failed to read replicated key from follower %s: %v", f.ID(), err)
		}
		if !bytes.Equal(fVal, val) {
			t.Errorf("follower %s value mismatch: expected %s, got %s", f.ID(), val, fVal)
		}
	}

	// 3. Test Master Failover
	// Stop the current Leader node
	leaderID := leader.ID()
	leader.Stop()

	// Wait for remaining 2 followers to detect heartbeat starvation and run election
	time.Sleep(800 * time.Millisecond)

	// Assert a new leader has emerged from the surviving 2 nodes (forming a quorum)
	var newLeader *consensus.Node
	newLeaderCount := 0
	var survivingFollower *consensus.Node

	for _, node := range nodes {
		if node.ID() == leaderID {
			continue // skip stopped node
		}
		if node.State() == consensus.Leader {
			newLeader = node
			newLeaderCount++
		} else if node.State() == consensus.Follower {
			survivingFollower = node
		}
	}

	if newLeaderCount != 1 {
		t.Fatalf("expected new leader to be elected after failover, got %d leaders", newLeaderCount)
	}

	// Verify new leader term is higher than the original term
	if newLeader.Term() <= leader.Term() {
		t.Errorf("expected new leader term (%d) to be higher than old leader term (%d)", newLeader.Term(), leader.Term())
	}

	// 4. Test writes to new Leader with remaining quorum
	failoverKey := db.Key("failoverKey")
	failoverVal := db.Value("failoverVal")

	// Quorum is met on 2 active nodes out of 3 total nodes
	err = newLeader.Put(failoverKey, failoverVal)
	if err != nil {
		t.Fatalf("failed to write to new leader: %v", err)
	}

	// Verify replication on the surviving follower
	time.Sleep(100 * time.Millisecond)
	sfVal, err := survivingFollower.Read(failoverKey)
	if err != nil {
		t.Fatalf("failed to read from surviving follower: %v", err)
	}
	if !bytes.Equal(sfVal, failoverVal) {
		t.Errorf("surviving follower value mismatch: expected %s, got %s", failoverVal, sfVal)
	}
}
