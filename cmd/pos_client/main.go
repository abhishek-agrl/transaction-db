package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func main() {
	serverAddr := "127.0.0.1:8000"
	logHeader := "[POS-CLIENT]"

	fmt.Printf("%s Connecting to TransactionDB server at %s...\n", logHeader, serverAddr)
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to database: %v", err))
	}
	defer conn.Close()

	// 1. Simulate batching 100 transaction records
	batchSize := 100
	keys := make([][]byte, batchSize)
	values := make([][]byte, batchSize)

	for i := 0; i < batchSize; i++ {
		// Key: tx_<nanosecond_timestamp>_<index>
		txID := fmt.Sprintf("tx_%d_%d", time.Now().UnixNano(), i)
		// Value: raw POS sales transaction JSON string
		txData := fmt.Sprintf(`{"pos_id":"register_42","amount":19.95,"currency":"USD","timestamp":"%s"}`, time.Now().Format(time.RFC3339Nano))
		
		keys[i] = []byte(txID)
		values[i] = []byte(txData)
	}

	// 2. Encode and stream using our binary wire protocol
	// Command Byte: 0x04 (BatchPut)
	_, err = conn.Write([]byte{0x04})
	if err != nil {
		panic(err)
	}

	// Write number of entries (4-byte big-endian)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(batchSize))
	_, err = conn.Write(lenBuf[:])
	if err != nil {
		panic(err)
	}

	// Stream key-value pairs sequentially
	for i := 0; i < batchSize; i++ {
		// Key Length + Key Bytes
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(keys[i])))
		conn.Write(lenBuf[:])
		conn.Write(keys[i])

		// Value Length + Value Bytes
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(values[i])))
		conn.Write(lenBuf[:])
		conn.Write(values[i])
	}

	// 3. Read response status byte
	statusBuf := make([]byte, 1)
	_, err = conn.Read(statusBuf)
	if err != nil {
		panic(fmt.Sprintf("Failed to read response status: %v", err))
	}

	if statusBuf[0] == 0x00 { // StatusSuccess
		fmt.Printf("%s Successfully ingested batch of %d POS transactions!\n", logHeader, batchSize)
	} else { // StatusError
		// Read Error Message Length and String
		_, err = conn.Read(lenBuf[:])
		if err != nil {
			panic(err)
		}
		errMsgLen := binary.BigEndian.Uint32(lenBuf[:])
		errMsg := make([]byte, errMsgLen)
		_, err = conn.Read(errMsg)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s Ingestion failed with error: %s\n", logHeader, string(errMsg))
	}
}
