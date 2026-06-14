package network

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	db "github.com/abhishek-agrl/transaction-db"
)

// Server handles TCP network connections and routes commands to the underlying storage database.
type Server struct {
	db       db.TransactionDB
	addr     string
	listener net.Listener
	wg       sync.WaitGroup
	mu       sync.Mutex
	closed   bool
	conns    map[net.Conn]struct{}
}

// NewServer creates a new network server instance configured to bind to the specified address.
func NewServer(addr string, database db.TransactionDB) *Server {
	return &Server{
		db:    database,
		addr:  addr,
		conns: make(map[net.Conn]struct{}),
	}
}

// Start opens the TCP listener socket and begins accepting client connections.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.listener = l
	s.closed = false
	s.mu.Unlock()

	// Accept loop runs in the background
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// acceptLoop continuously accepts incoming client connections until the server is closed.
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			isClosed := s.closed
			s.mu.Unlock()
			if isClosed {
				return // Clean exit on server Stop()
			}
			continue
		}

		s.mu.Lock()
		if s.closed {
			conn.Close()
			s.mu.Unlock()
			return
		}
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles incoming messages on a single client connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()

	for {
		// 1. Read command type (1 byte)
		var cmdBuf [1]byte
		if _, err := io.ReadFull(conn, cmdBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return // Clean client disconnect
			}
			return
		}
		cmd := cmdBuf[0]

		// 2. Dispatch command
		var err error
		switch cmd {
		case CmdPut:
			err = s.handlePut(conn)
		case CmdRead:
			err = s.handleRead(conn)
		case CmdReadKeyRange:
			err = s.handleReadKeyRange(conn)
		case CmdBatchPut:
			err = s.handleBatchPut(conn)
		case CmdDelete:
			err = s.handleDelete(conn)
		default:
			s.writeError(conn, ErrUnknownCommand)
			return // Protocol violation: abort connection
		}

		if err != nil {
			return // Socket read/write failure
		}
	}
}

// handlePut processes a Put command frame.
func (s *Server) handlePut(conn net.Conn) error {
	key, err := readPrefixedBytes(conn)
	if err != nil {
		return err
	}
	value, err := readPrefixedBytes(conn)
	if err != nil {
		return err
	}

	dbErr := s.db.Put(key, value)
	if dbErr != nil {
		return s.writeError(conn, dbErr)
	}

	return s.writeSuccess(conn)
}

// handleRead processes a Read command frame.
func (s *Server) handleRead(conn net.Conn) error {
	key, err := readPrefixedBytes(conn)
	if err != nil {
		return err
	}

	value, dbErr := s.db.Read(key)
	if dbErr != nil {
		return s.writeError(conn, dbErr)
	}

	if _, err := conn.Write([]byte{StatusSuccess}); err != nil {
		return err
	}
	return writePrefixedBytes(conn, value)
}

// handleReadKeyRange processes a ReadKeyRange command frame.
func (s *Server) handleReadKeyRange(conn net.Conn) error {
	startKey, err := readPrefixedBytes(conn)
	if err != nil {
		return err
	}
	endKey, err := readPrefixedBytes(conn)
	if err != nil {
		return err
	}

	entries, dbErr := s.db.ReadKeyRange(startKey, endKey)
	if dbErr != nil {
		return s.writeError(conn, dbErr)
	}

	if _, err := conn.Write([]byte{StatusSuccess}); err != nil {
		return err
	}

	// Write number of entries (4 bytes big-endian)
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(entries)))
	if _, err := conn.Write(countBuf[:]); err != nil {
		return err
	}

	// Write each Key-Value pair
	for _, entry := range entries {
		if err := writePrefixedBytes(conn, entry.Key); err != nil {
			return err
		}
		if err := writePrefixedBytes(conn, entry.Value); err != nil {
			return err
		}
	}

	return nil
}

// handleBatchPut processes a BatchPut command frame.
func (s *Server) handleBatchPut(conn net.Conn) error {
	var countBuf [4]byte
	if _, err := io.ReadFull(conn, countBuf[:]); err != nil {
		return err
	}
	count := binary.BigEndian.Uint32(countBuf[:])

	keys := make([]db.Key, count)
	values := make([]db.Value, count)

	for i := uint32(0); i < count; i++ {
		k, err := readPrefixedBytes(conn)
		if err != nil {
			return err
		}
		v, err := readPrefixedBytes(conn)
		if err != nil {
			return err
		}
		keys[i] = k
		values[i] = v
	}

	dbErr := s.db.BatchPut(keys, values)
	if dbErr != nil {
		return s.writeError(conn, dbErr)
	}

	return s.writeSuccess(conn)
}

// handleDelete processes a Delete command frame.
func (s *Server) handleDelete(conn net.Conn) error {
	key, err := readPrefixedBytes(conn)
	if err != nil {
		return err
	}

	dbErr := s.db.Delete(key)
	if dbErr != nil {
		return s.writeError(conn, dbErr)
	}

	return s.writeSuccess(conn)
}

// writeSuccess sends a success status back to the client.
func (s *Server) writeSuccess(conn net.Conn) error {
	_, err := conn.Write([]byte{StatusSuccess})
	return err
}

// writeError sends an error status and the error message string back to the client.
func (s *Server) writeError(conn net.Conn, err error) error {
	if _, writeErr := conn.Write([]byte{StatusError}); writeErr != nil {
		return writeErr
	}
	return writePrefixedBytes(conn, []byte(err.Error()))
}

// Stop closes the listener socket and cleans up active client connections.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}

	// Close all active connections to unblock goroutines
	for conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()

	// Wait for all client handler goroutines to finish
	s.wg.Wait()
	return err
}

// Addr returns the server's listener network address, or nil if the server is not listening.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}
