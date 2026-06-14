package tests

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	db "github.com/abhishek-agrl/transaction-db"
	"github.com/abhishek-agrl/transaction-db/network"
	"github.com/abhishek-agrl/transaction-db/storage"
)

// Helper client methods to write frames and decode responses over TCP

func writePrefixedBytes(w io.Writer, b []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readPrefixedBytes(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func readResponseStatus(r io.Reader) (byte, error) {
	var statusBuf [1]byte
	if _, err := io.ReadFull(r, statusBuf[:]); err != nil {
		return 0, err
	}
	return statusBuf[0], nil
}

func readResponseError(r io.Reader) (string, error) {
	errMsg, err := readPrefixedBytes(r)
	if err != nil {
		return "", err
	}
	return string(errMsg), nil
}

// Client RPC wrapper functions for tests
func clientPut(conn net.Conn, key, val []byte) (byte, error) {
	if _, err := conn.Write([]byte{network.CmdPut}); err != nil {
		return 0, err
	}
	if err := writePrefixedBytes(conn, key); err != nil {
		return 0, err
	}
	if err := writePrefixedBytes(conn, val); err != nil {
		return 0, err
	}

	return readResponseStatus(conn)
}

func clientRead(conn net.Conn, key []byte) (byte, []byte, error) {
	if _, err := conn.Write([]byte{network.CmdRead}); err != nil {
		return 0, nil, err
	}
	if err := writePrefixedBytes(conn, key); err != nil {
		return 0, nil, err
	}

	status, err := readResponseStatus(conn)
	if err != nil {
		return 0, nil, err
	}

	if status == network.StatusError {
		errMsg, err := readResponseError(conn)
		return status, []byte(errMsg), err
	}

	val, err := readPrefixedBytes(conn)
	return status, val, err
}

func clientDelete(conn net.Conn, key []byte) (byte, error) {
	if _, err := conn.Write([]byte{network.CmdDelete}); err != nil {
		return 0, err
	}
	if err := writePrefixedBytes(conn, key); err != nil {
		return 0, err
	}

	return readResponseStatus(conn)
}

func clientReadKeyRange(conn net.Conn, start, end []byte) (byte, []db.Entry, error) {
	if _, err := conn.Write([]byte{network.CmdReadKeyRange}); err != nil {
		return 0, nil, err
	}
	if err := writePrefixedBytes(conn, start); err != nil {
		return 0, nil, err
	}
	if err := writePrefixedBytes(conn, end); err != nil {
		return 0, nil, err
	}

	status, err := readResponseStatus(conn)
	if err != nil {
		return 0, nil, err
	}

	if status == network.StatusError {
		errMsg, err := readResponseError(conn)
		return status, []db.Entry{{Key: db.Key(errMsg)}}, err
	}

	// Read entry count
	var countBuf [4]byte
	if _, err := io.ReadFull(conn, countBuf[:]); err != nil {
		return 0, nil, err
	}
	count := binary.BigEndian.Uint32(countBuf[:])

	entries := make([]db.Entry, count)
	for i := uint32(0); i < count; i++ {
		k, err := readPrefixedBytes(conn)
		if err != nil {
			return 0, nil, err
		}
		v, err := readPrefixedBytes(conn)
		if err != nil {
			return 0, nil, err
		}
		entries[i] = db.Entry{Key: k, Value: v}
	}

	return status, entries, nil
}

func clientBatchPut(conn net.Conn, keys, values [][]byte) (byte, error) {
	if _, err := conn.Write([]byte{network.CmdBatchPut}); err != nil {
		return 0, err
	}

	// Write number of entries
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(keys)))
	if _, err := conn.Write(countBuf[:]); err != nil {
		return 0, err
	}

	for i := range keys {
		if err := writePrefixedBytes(conn, keys[i]); err != nil {
			return 0, err
		}
		if err := writePrefixedBytes(conn, values[i]); err != nil {
			return 0, err
		}
	}

	return readResponseStatus(conn)
}

// Tests

func TestNetworkServerLifecycleAndOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "network_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 1. Initialize storage and start server on dynamic ephemeral port
	engine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	srv := network.NewServer("127.0.0.1:0", engine)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	addr := srv.Addr().String()

	// 2. Connect client dial
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	// 3. Test Put
	status, err := clientPut(conn, []byte("hello"), []byte("world"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if status != network.StatusSuccess {
		t.Errorf("expected Put Success, got status %x", status)
	}

	// 4. Test Read
	status, val, err := clientRead(conn, []byte("hello"))
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if status != network.StatusSuccess {
		t.Errorf("expected Read Success, got status %x", status)
	}
	if !bytes.Equal(val, []byte("world")) {
		t.Errorf("expected 'world', got '%s'", val)
	}

	// 5. Test Read (not found)
	status, val, err = clientRead(conn, []byte("not_exist"))
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if status != network.StatusError {
		t.Errorf("expected error status, got %x", status)
	}
	if !bytes.Contains(val, []byte(db.ErrKeyNotFound.Error())) {
		t.Errorf("expected key not found error message, got '%s'", val)
	}

	// 6. Test Delete
	status, err = clientDelete(conn, []byte("hello"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if status != network.StatusSuccess {
		t.Errorf("expected Delete Success, got status %x", status)
	}

	// Verify key was deleted
	status, _, err = clientRead(conn, []byte("hello"))
	if err != nil {
		t.Fatalf("Read after delete failed: %v", err)
	}
	if status != network.StatusError {
		t.Errorf("expected Read after delete to fail, got status %x", status)
	}

	// 7. Test BatchPut
	keys := [][]byte{[]byte("b1"), []byte("b2"), []byte("b3")}
	vals := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}
	status, err = clientBatchPut(conn, keys, vals)
	if err != nil {
		t.Fatalf("BatchPut failed: %v", err)
	}
	if status != network.StatusSuccess {
		t.Errorf("expected BatchPut Success, got status %x", status)
	}

	// 8. Test ReadKeyRange
	status, entries, err := clientReadKeyRange(conn, []byte("b1"), []byte("b3"))
	if err != nil {
		t.Fatalf("ReadKeyRange failed: %v", err)
	}
	if status != network.StatusSuccess {
		t.Fatalf("expected Range Success, got status %x", status)
	}
	// b3 is exclusive, so range should return b1, b2
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if !bytes.Equal(entries[0].Key, []byte("b1")) || !bytes.Equal(entries[1].Key, []byte("b2")) {
		t.Errorf("unexpected range entries returned: %+v", entries)
	}
}

func TestNetworkServerMalformedRequestDisconnects(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "network_malformed_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	srv := network.NewServer("127.0.0.1:0", engine)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Send an invalid command code (e.g. 0xFF)
	if _, err := conn.Write([]byte{0xFF}); err != nil {
		t.Fatalf("failed to write bad cmd: %v", err)
	}

	// Server should reply with StatusError, error message, and then terminate the connection.
	status, err := readResponseStatus(conn)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	if status != network.StatusError {
		t.Errorf("expected StatusError, got %x", status)
	}

	errMsg, err := readResponseError(conn)
	if err != nil {
		t.Fatalf("failed to read error payload: %v", err)
	}
	if errMsg == "" {
		t.Error("expected non-empty error message")
	}

	// Verify server terminated the connection (Read should yield EOF)
	oneByte := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err = conn.Read(oneByte)
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected server to close connection with EOF, got: %v", err)
	}
}
