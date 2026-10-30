package swarm

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"p2p/pkg/bitfield"
	"p2p/pkg/protocol"
)

// BlockData holds chunk payload received from a remote peer.
type BlockData struct {
	Index int
	Begin int
	Data  []byte
}

// PeerConnection manages a direct wire session with a single peer.
type PeerConnection struct {
	Conn           net.Conn
	PeerID         [20]byte
	Address        string
	InfoHash       [32]byte
	stateMu        sync.RWMutex
	bitfield       *bitfield.Bitfield
	choked         bool
	interested     bool
	incomingBlocks chan *BlockData
	writeMu        sync.Mutex
	closed         chan struct{}
	closeOnce      sync.Once
}

// NewPeerConnection initializes peer state around an established connection.
func NewPeerConnection(conn net.Conn, address string, infoHash [32]byte, totalPieces int) *PeerConnection {
	return &PeerConnection{
		Conn:           conn,
		Address:        address,
		InfoHash:       infoHash,
		bitfield:       bitfield.New(totalPieces),
		choked:         true, // Initially choked until unchoke received
		interested:     false,
		incomingBlocks: make(chan *BlockData, 128),
		closed:         make(chan struct{}),
	}
}

// DispatchBlock queues an incoming piece block to the waiting downloader.
func (p *PeerConnection) DispatchBlock(block *BlockData) {
	select {
	case p.incomingBlocks <- block:
	default:
	}
}

// WaitForBlock waits for the next block data delivered to this peer.
func (p *PeerConnection) WaitForBlock(ctx context.Context, timeout time.Duration) (*BlockData, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.closed:
		return nil, errors.New("peer connection closed")
	case <-timer.C:
		return nil, errors.New("timed out waiting for piece block")
	case block := <-p.incomingBlocks:
		return block, nil
	}
}

// SetChoked updates the choked state thread-safely.
func (p *PeerConnection) SetChoked(choked bool) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.choked = choked
}

// IsChoked returns true if peer is currently choking us.
func (p *PeerConnection) IsChoked() bool {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.choked
}

// SetInterested updates the interested state thread-safely.
func (p *PeerConnection) SetInterested(interested bool) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.interested = interested
}

// IsInterested returns true if peer is interested in our pieces.
func (p *PeerConnection) IsInterested() bool {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.interested
}

// SetBitfield replaces the peer's bitfield thread-safely.
func (p *PeerConnection) SetBitfield(bf *bitfield.Bitfield) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.bitfield = bf
}

// SetPiece sets a bit in the peer's bitfield thread-safely.
func (p *PeerConnection) SetPiece(idx int) {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	if p.bitfield != nil {
		p.bitfield.SetPiece(idx)
	}
}

// HasPiece checks if the peer possesses piece index thread-safely.
func (p *PeerConnection) HasPiece(idx int) bool {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	if p.bitfield == nil {
		return false
	}
	return p.bitfield.HasPiece(idx)
}

// PerformHandshake sends our handshake and validates the remote handshake.
func (p *PeerConnection) PerformHandshake(ourPeerID [20]byte, timeout time.Duration) error {
	_ = p.Conn.SetDeadline(time.Now().Add(timeout))
	defer p.Conn.SetDeadline(time.Time{})

	ourHS := &protocol.Handshake{
		Pstr:     protocol.ProtocolIdentifier,
		InfoHash: p.InfoHash,
		PeerID:   ourPeerID,
	}

	writeErrChan := make(chan error, 1)
	go func() {
		_, err := p.Conn.Write(ourHS.Serialize())
		writeErrChan <- err
	}()

	remoteHS, err := protocol.ReadHandshake(p.Conn)
	if err != nil {
		return fmt.Errorf("failed reading remote handshake: %w", err)
	}

	if writeErr := <-writeErrChan; writeErr != nil {
		return fmt.Errorf("failed sending handshake: %w", writeErr)
	}

	if !ourHS.Matches(remoteHS) {
		return fmt.Errorf("info hash mismatch with peer %s", p.Address)
	}

	p.PeerID = remoteHS.PeerID
	return nil
}

// SendMessage writes a framed message to the peer connection thread-safely.
func (p *PeerConnection) SendMessage(msg *protocol.Message) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	_ = p.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	data := msg.Serialize()
	_, err := p.Conn.Write(data)
	return err
}

// ReadMessage waits for and parses the next wire message.
func (p *PeerConnection) ReadMessage(timeout time.Duration) (*protocol.Message, error) {
	if timeout > 0 {
		_ = p.Conn.SetReadDeadline(time.Now().Add(timeout))
	} else {
		_ = p.Conn.SetReadDeadline(time.Time{})
	}
	return protocol.ReadMessage(p.Conn)
}

// Close closes the connection cleanly.
func (p *PeerConnection) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.closed)
		if p.Conn != nil {
			err = p.Conn.Close()
		}
	})
	return err
}

// IsClosed returns true if connection is terminated.
func (p *PeerConnection) IsClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}
