package protocol

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

const ProtocolIdentifier = "P2P-PROTOCOL-V1"

// Handshake represents the initial connection message between two peers.
type Handshake struct {
	Pstr     string
	InfoHash [32]byte
	PeerID   [20]byte
}

// GeneratePeerID creates a random 20-byte peer ID with a prefix.
func GeneratePeerID() [20]byte {
	var id [20]byte
	prefix := []byte("-P2P001-")
	copy(id[:], prefix)
	_, _ = rand.Read(id[len(prefix):])
	return id
}

// PeerIDString returns a string representation of the peer ID.
func PeerIDString(id [20]byte) string {
	return hex.EncodeToString(id[:])
}

// InfoHashFromHex parses a 64-character hex string into a 32-byte array.
func InfoHashFromHex(hashHex string) ([32]byte, error) {
	var out [32]byte
	b, err := hex.DecodeString(hashHex)
	if err != nil {
		return out, fmt.Errorf("invalid hex info hash: %w", err)
	}
	if len(b) != 32 {
		return out, fmt.Errorf("info hash must be 32 bytes, got %d", len(b))
	}
	copy(out[:], b)
	return out, nil
}

// Serialize encodes the handshake into a binary slice.
func (h *Handshake) Serialize() []byte {
	buf := make([]byte, 1+len(h.Pstr)+8+32+20)
	buf[0] = byte(len(h.Pstr))
	curr := 1
	curr += copy(buf[curr:], h.Pstr)
	curr += 8 // 8 reserved zero bytes
	curr += copy(buf[curr:], h.InfoHash[:])
	copy(buf[curr:], h.PeerID[:])
	return buf
}

// ReadHandshake reads and parses a handshake from an io.Reader.
func ReadHandshake(r io.Reader) (*Handshake, error) {
	lengthBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, fmt.Errorf("failed to read handshake length: %w", err)
	}

	pstrlen := int(lengthBuf[0])
	if pstrlen == 0 {
		return nil, errors.New("invalid handshake length 0")
	}

	handshakeBuf := make([]byte, pstrlen+8+32+20)
	if _, err := io.ReadFull(r, handshakeBuf); err != nil {
		return nil, fmt.Errorf("failed to read handshake body: %w", err)
	}

	pstr := string(handshakeBuf[:pstrlen])
	if pstr != ProtocolIdentifier {
		return nil, fmt.Errorf("unsupported protocol: %s", pstr)
	}

	var infoHash [32]byte
	var peerID [20]byte

	offset := pstrlen + 8
	copy(infoHash[:], handshakeBuf[offset:offset+32])
	offset += 32
	copy(peerID[:], handshakeBuf[offset:offset+20])

	return &Handshake{
		Pstr:     pstr,
		InfoHash: infoHash,
		PeerID:   peerID,
	}, nil
}

// Equal returns whether two handshakes match in InfoHash.
func (h *Handshake) Matches(other *Handshake) bool {
	return bytes.Equal(h.InfoHash[:], other.InfoHash[:])
}
