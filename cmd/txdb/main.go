package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/abhishek-agrl/transaction-db/consensus"
	"github.com/abhishek-agrl/transaction-db/network"
	"github.com/abhishek-agrl/transaction-db/storage"
)

func main() {
	// 1. Define command-line arguments
	nodeID := flag.String("id", "node_0", "Unique identifier for this cluster node")
	clientAddr := flag.String("addr", "127.0.0.1:8000", "TCP port for client database commands")
	rpcAddr := flag.String("rpc", "127.0.0.1:9000", "TCP port for inter-node replication and consensus")
	dbDir := flag.String("dir", "./data_0", "Directory to store WAL and SSTable files")
	peersList := flag.String("peers", "", "Comma-separated list of peer RPC addresses (e.g. 127.0.0.1:9001,127.0.0.1:9002)")
	flag.Parse()

	log.Printf("[%s] Initializing TransactionDB...", *nodeID)

	// 2. Parse peer list
	var peers []string
	if *peersList != "" {
		for _, peer := range strings.Split(*peersList, ",") {
			trimmed := strings.TrimSpace(peer)
			if trimmed != "" {
				peers = append(peers, trimmed)
			}
		}
	}

	// 3. Initialize local Storage Engine
	engine, err := storage.NewEngine(*dbDir)
	if err != nil {
		log.Fatalf("Failed to initialize storage engine: %v", err)
	}
	defer engine.Close()
	log.Printf("[%s] Storage engine loaded successfully in %s", *nodeID, *dbDir)

	// 4. Initialize Consensus Node
	node := consensus.NewNode(*nodeID, *rpcAddr, peers, engine)
	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start consensus node: %v", err)
	}
	defer node.Stop()
	log.Printf("[%s] Consensus node listening for peers at %s (Peers: %v)", *nodeID, *rpcAddr, peers)

	// 5. Initialize TCP Network Server
	// We pass the consensus node (which implements db.TransactionDB) directly
	// so client writes are automatically replicated across the quorum.
	srv := network.NewServer(*clientAddr, node)
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start network server: %v", err)
	}
	defer srv.Stop()
	log.Printf("[%s] Database server accepting client connections at %s", *nodeID, *clientAddr)

	// 6. Handle OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan

	log.Printf("[%s] Shutting down database server (received signal %v)...", *nodeID, sig)
	fmt.Println("Graceful shutdown completed successfully. Goodbye!")
}
