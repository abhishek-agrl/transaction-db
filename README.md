# TransactionDB

A custom, network-available, persistent, and replicated Key/Value storage engine built from scratch using **only the Go standard library**.

TransactionDB utilizes a **Log-Structured Merge-Tree (LSM-Tree)** storage layout, exposes its API over a custom **TCP binary wire protocol**, and synchronizes state across a cluster using a custom consensus algorithm called **"Raft-Lite"**.

---

## 🏛️ System Architecture

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

## 🛠️ Implementation Details & Design Decisions

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

## ⚖️ Architectural Trade-offs

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

## ⚠️ Shortcomings & Future Work

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

## 🚦 Getting Started

### Prerequisites
* Go 1.22+ installed.

### Run Tests
To run all unit and integration tests:
```bash
go test -v ./...
```

### Run Concurrency & Race Tests
To run the stress tests and verify memory/race safety:
```bash
go test -race -v ./...
```
