# TransactionDB

A custom, network-available, persistent, and replicated Key/Value storage engine built from scratch using **only the Go standard library**.

TransactionDB utilizes a **Log-Structured Merge-Tree (LSM-Tree)** storage layout, exposes its API over a custom **TCP binary wire protocol**, and synchronizes state across a cluster using a custom implementation of the "RAFT" consensus algorithm.

---

## System Architecture

TransactionDB is composed of three primary layers: **Storage Engine**, **Networking**, and **Consensus**.

```
                        ┌──────────────────────────────┐
                        │      Client Request          │
                        └──────────────┬───────────────┘
                                       │ (TCP Protocol)
                                       ▼
                        ┌──────────────────────────────┐
                        │      TCP Network Server      │
                        └──────────────┬───────────────┘
                                       │ (Exposes db.TransactionDB API)
                                       ▼
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                            Consensus Node                                │
 │                                                                          │
 │     ┌───────────────┐ (ReplicateData) ┌───────────────┐                  │
 │     │  Consensus    ├────────────────►│  Peer Node(s) │                  │
 │     │  Coordinator  │                 └───────────────┘                  │
 │     └───────┬───────┘                                                    │
 └─────────────┼────────────────────────────────────────────────────────────┘
               │ (Commits writes on Quorum Success)
               ▼
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                            Storage Engine                                │
 │                                                                          │
 │                     ┌──────────────────────────────┐                     │
 │                     │    Engine (RWMutex Lock)     │                     │
 │                     └───────┬──────────────┬───────┘                     │
 │                             │              │                             │
 │                             ▼              ▼                             │
 │                     ┌──────────────┐ ┌──────────────┐                    │
 │                     │  Active WAL  │ │ Active Mem   │                    │
 │                     │   (Disk)     │ │ (Skip List)  │                    │
 │                     └──────────────┘ └──────┬───────┘                    │
 │                                             │                            │
 │                                             ▼ (Flush size >= limit)      │
 │                                      ┌──────────────┐                    │
 │                                      │   SSTable    │                    │
 │                                      │ (sstable.db) │◄───[Sparse Index]  │
 │                                      └──────────────┘                    │
 └──────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Details & Design Decisions

### 1. Zero-Allocation Domain Model ([db.go](file:///Users/abhishek/Developer/transaction-db/db.go))
To maximize execution speed under high write volumes, `Key` and `Value` are defined as raw byte slices (`[]byte`). This avoids the garbage collection overhead and memory allocation spikes associated with constantly converting between Go `string` and `[]byte` types.

### 2. Memtable Skip List ([storage/skiplist.go](file:///Users/abhishek/Developer/transaction-db/storage/skiplist.go))
* **Why Skip List?** Standard Go maps do not preserve sorted order, which is necessary for range scans. Rather than utilizing complex self-balancing binary search trees (like Red-Black Trees), we implemented a **Skip List**. Its pointer-based shortcut layer design allows $O(\log N)$ search and insertion times with a simpler balancing strategy based on coin-flip probabilities.
* **Tombstones:** Deletions do not immediately reclaim memory. Instead, we insert a node with an `isDeleted = true` flag (Tombstone).
* **Concurrency:** Thread-safety is achieved by locking the Skip List with a `sync.RWMutex` (multiple simultaneous readers, single writer).

### 3. Durability & Recovery: WAL ([storage/wal.go](file:///Users/abhishek/Developer/transaction-db/storage/wal.go))
* **Durability:** Writes are logged sequentially to an append-only Write-Ahead Log (WAL) file. We call `file.Sync()` (`fsync`) on every write to guarantee data persistence to physical media before replying to clients.
* **Layout:** Format per log entry: `[Key Length (4B)][Key Bytes][Value Length (4B)][Value Bytes]`. Deletions are logged with a value length of `-1`.
* **Fast Recovery:** On startup, we parse the WAL from byte `0` to `EOF` to populate the Memtable, discarding partial or corrupted entries (returning `ErrCorruptRecord`).

### 4. Disk Scaling: SSTables & Sparse Indexes ([storage/sstable.go](file:///Users/abhishek/Developer/transaction-db/storage/sstable.go))
* **Memtable Flush:** When the Skip List reaches a limit (e.g. `1000` entries), it flushes sorted elements to an SSTable file on disk (`sstable_<id>.db`). The old WAL is deleted, and a new one is opened.
* **Sparse Indexing:** To avoid loading entire SSTables into memory, we record the file offset of every $16$-th key in a RAM-based sparse index. During reads, we binary search the sparse index to find the target block, seek there, and scan sequentially.
* **Range Merge:** Range queries (`ReadKeyRange`) scan the active Memtable and all SSTables from oldest to newest, merge them into a map (allowing newer keys to overwrite older ones), sort the keys lexicographically, and filter out tombstones.

### 5. Networking: TCP server & Binary Wire Protocol ([network/](file:///Users/abhishek/Developer/transaction-db/network/))
* **Wire Protocol:** To bypass the latency of serialization formats like JSON, the server communicates via a custom binary protocol:
  * **Request:** `[Command Byte (1B)][Arg 1 Length (4B)][Arg 1 Bytes]...`
  * **Response:** `[Status Byte (1B)][Payload Length (4B)][Payload Bytes]`
* **Concurrency & Shutdown:** The server accepts TCP connections in a loop and immediately spawns a goroutine per client connection. A safe shutdown routine (`Stop`) closes active sockets, releases listeners, and blocks on a `sync.WaitGroup` until handler routines exit cleanly.

### 6. Consensus: "Raft-Lite" Replication ([consensus/consensus.go](file:///Users/abhishek/Developer/transaction-db/consensus/consensus.go))
* **Heartbeat Elections:** Follower nodes monitor Leader heartbeats. If starved past a randomized timer (**150ms to 300ms**), they promote to Candidates, increment the term, and request votes. The randomized timeout window prevents split-vote deadlock loops.
* **Quorum Writes:** Clients submit writes to the Leader. The Leader replicates mutations to peers via the `ReplicateData` RPC. The Leader only commits locally to its disk and Memtable if a **strict majority quorum ($N/2 + 1$)** of nodes successfully write and acknowledge the replication payload.

---

## Architectural Trade-offs

### 1. Synchronous vs. Background Flushing
* **Design Decision:** In our implementation, when the Memtable threshold is reached, flushing to an SSTable is executed **synchronously** as part of the client write transaction.
* **Trade-off:** This simplifies concurrency safety and ensures we do not run out of memory during a sudden flood of writes. However, it introduces a **write stall**—the client that triggers the flush pays a latency penalty because they must wait for the entire Memtable to be written to disk. In a production engine, this is typically offloaded to a background goroutine while new writes append to a secondary active Memtable.

### 2. Quorum Primary-Backup (Raft-Lite) vs. Academic Raft
* **Design Decision:** Rather than implementing a strict Raft consensus engine (which requires index tracking, uncommitted log truncations, and index repairs), we built a **Raft-Lite** protocol.
* **Trade-off:** Raft-Lite dramatically simplifies the codebase and minimizes term conflict resolutions, making it highly maintainable. However, it assumes that nodes are generally well-behaved. If a partition splits a cluster for a long time, syncing is done by catching up missing records sequentially; it does not support complex index matching/log repairs.

### 3. Engine-Level Locking vs. Lock-Free Engines
* **Design Decision:** All write transactions hold an exclusive write lock on the `Engine` coordinator.
* **Trade-off:** This guarantees strict write-ordering consistency (essential for recovering state in the correct sequence). The trade-off is a bottleneck on concurrent writes, as only one transaction can write to the WAL and update the Memtable at a time. High-performance databases often utilize lock-free indexes or partitioned bucket locks to increase parallel write throughput.

---

## Shortcomings & Future Work

### 1. Read Amplification (Lack of Compaction)
* **Shortcoming:** Because we flush Memtables to disk but never merge them, the number of SSTable files grows indefinitely over time. To search for a missing key, the engine must perform a point lookup on the Memtable and then on *every single SSTable* from newest to oldest. This creates severe **read amplification** and degrades read latency.
* **Future Work:** Implement a **Compaction Subsystem** (e.g. Size-Tiered or Leveled Compaction) to periodically merge multiple small SSTables into larger ones, reclaiming space by purging duplicate updates and obsolete tombstones.

### 2. Lack of Bloom Filters
* **Shortcoming:** When searching for a key that does not exist in the database, we must query every SSTable file, causing unnecessary disk seeks.
* **Future Work:** Generate a **Bloom Filter** for each SSTable. The filter can be held entirely in RAM. By checking it first, we can instantly bypass looking in SSTables that definitely do not contain the key, eliminating waste disk I/O.

### 3. Dynamic Snapshot State Transfer
* **Shortcoming:** If a follower is offline for a very long period, the Leader will have already flushed the logs and truncated its WAL. Currently, the follower cannot catch up automatically via log replay.
* **Future Work:** Implement a network-based **Snapshot Transfer** RPC. When the Leader detects a follower is too far behind, it should package its active SSTables and transfer them to the follower, which loads them directly.

### 4. Dynamic Cluster Membership Changes
* **Shortcoming:** The peer list of nodes is currently static and defined at boot. Adding or removing a node from the cluster requires restarting all running nodes.
* **Future Work:** Implement a dynamic membership protocol (such as Raft's joint consensus) to allow adding/removing nodes via configuration RPCs without downtime.

---

## Getting Started & Node Discovery

### 1. How Nodes Find Each Other (Peer Discovery)
Nodes do not require dynamic service discovery (like Consul/DNS) at boot. Instead, they rely on a static configuration list passed via the `-peers` flag:
* **Configuration:** Every node receives the exact RPC addresses of its peer cluster members at startup.
* **On-Demand Socket Connections:** When a candidate solicits votes or a leader replicates data, it dials the peer's RPC port on-demand using standard `net/rpc` over TCP (`rpc.Dial`).
* **Fault Tolerance & Self-Healing:** Because connections are dialed on-demand and closed immediately after RPC execution, nodes do not crash if peers are temporarily offline. If a node crashes and reboots, peers will automatically reconnect and discover it on the next heartbeat/replication cycle.

---

### 2. Compilation
Compile the CLI server and the POS simulation client binaries using the standard Go compiler:
```bash
# Compile Server
go build -o txdb cmd/txdb/main.go

# Compile POS Client Simulator
go build -o pos_client cmd/pos_client/main.go
```

---

### 3. Run a 3-Node Cluster locally
To spin up a local 3-node replicated cluster, open three separate terminal windows and run the following commands:

#### Terminal 1: Node 0
Exposes client TCP database listener on port `8000`, consensus RPC listener on port `9000`, and links to peers `9001` and `9002`:
```bash
./txdb -id node_0 -addr 127.0.0.1:8000 -rpc 127.0.0.1:9000 -dir ./data_0 -peers 127.0.0.1:9001,127.0.0.1:9002
```

#### Terminal 2: Node 1
Exposes client TCP database listener on port `8001`, consensus RPC listener on port `9001`, and links to peers `9000` and `9002`:
```bash
./txdb -id node_1 -addr 127.0.0.1:8001 -rpc 127.0.0.1:9001 -dir ./data_1 -peers 127.0.0.1:9000,127.0.0.1:9002
```

#### Terminal 3: Node 2
Exposes client TCP database listener on port `8002`, consensus RPC listener on port `9002`, and links to peers `9000` and `9001`:
```bash
./txdb -id node_2 -addr 127.0.0.1:8002 -rpc 127.0.0.1:9002 -dir ./data_2 -peers 127.0.0.1:9000,127.0.0.1:9001
```

Once started, the nodes will automatically run Term election tickers. One node will be elected Leader and emit heartbeats, while the others will assume Follower roles.

---

### 4. Run the Test Suites
* **Run All Tests:**
  ```bash
  go test -v ./...
  ```
* **Run with Go Race Detector:**
  ```bash
  go test -race -v ./...
  ```

