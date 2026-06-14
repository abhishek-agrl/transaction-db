package network

import (
	"encoding/binary"
	"errors"
	"io"
)

// Command operation codes (1 byte)
const (
	CmdPut          byte = 0x01
	CmdRead         byte = 0x02
	CmdReadKeyRange byte = 0x03
	CmdBatchPut     byte = 0x04
	CmdDelete       byte = 0x05
)

// Response status codes (1 byte)
const (
	StatusSuccess byte = 0x00
	StatusError   byte = 0x01
)

var (
	ErrUnknownCommand = errors.New("protocol: unknown command")
)

// readPrefixedBytes reads a 4-byte big-endian length prefix and then reads exactly that many bytes from the stream.
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

// writePrefixedBytes writes a 4-byte big-endian length prefix followed by the raw bytes.
func writePrefixedBytes(w io.Writer, b []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}
