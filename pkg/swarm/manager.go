package swarm

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"p2p/pkg/bitfield"
	"p2p/pkg/protocol"
	"p2p/pkg/storage"
)

// BlockSize is the standard 16 KB chunk transfer slice.
const BlockSize = 16 * 1024

// SwarmStats reports live metrics from the swarm.
type SwarmStats struct {
	TotalPieces     int
	CompletedPieces int
	ActivePeers     int
	DownloadedBytes int64
	TotalBytes      int64
	DownloadRateBps float64
}

// SwarmManager coordinates multi-peer downloading, scheduling, and seeding.
type SwarmManager struct {
	PeerID          [20]byte
	InfoHash        [32]byte
	Storage         *storage.TorrentFile
	mu              sync.RWMutex
	peers           map[string]*PeerConnection
	inFlight        map[int]struct{}
	downloadedBytes atomic.Int64
	doneChan        chan struct{}
	closeOnce       sync.Once
	tcpListener     net.Listener
	closed          bool
}

// NewSwarmManager initializes a swarm manager for a torrent storage file.
func NewSwarmManager(peerID [20]byte, st *storage.TorrentFile) (*SwarmManager, error) {
	infoHash, err := protocol.InfoHashFromHex(st.Manifest.InfoHash)
	if err != nil {
		return nil, fmt.Errorf("invalid manifest infohash: %w", err)
	}

	return &SwarmManager{
		PeerID:   peerID,
		InfoHash: infoHash,
		Storage:  st,
		peers:    make(map[string]*PeerConnection),
		inFlight: make(map[int]struct{}),
		doneChan: make(chan struct{}),
	}, nil
}

// Done returns a channel signaling download completion.
func (sm *SwarmManager) Done() <-chan struct{} {
	return sm.doneChan
}

// Stats returns current progress and swarm metrics.
func (sm *SwarmManager) Stats() SwarmStats {
	sm.mu.RLock()
	activePeers := len(sm.peers)
	sm.mu.RUnlock()

	completed := sm.Storage.Bitfield.Count()
	total := sm.Storage.Manifest.PieceCount

	return SwarmStats{
		TotalPieces:     total,
		CompletedPieces: completed,
		ActivePeers:     activePeers,
		DownloadedBytes: sm.downloadedBytes.Load(),
		TotalBytes:      sm.Storage.Manifest.FileSize,
	}
}

// StartListener starts listening for incoming peer connections on the given port.
func (sm *SwarmManager) StartListener(port int) (int, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return 0, fmt.Errorf("failed to start swarm listener: %w", err)
	}
	sm.tcpListener = listener
	actualPort := listener.Addr().(*net.TCPAddr).Port

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go sm.HandleIncomingConn(conn)
		}
	}()

	return actualPort, nil
}

// HandleIncomingConn processes an inbound connection from a remote peer.
func (sm *SwarmManager) HandleIncomingConn(conn net.Conn) {
	peerAddr := conn.RemoteAddr().String()
	peer := NewPeerConnection(conn, peerAddr, sm.InfoHash, sm.Storage.Manifest.PieceCount)

	if err := peer.PerformHandshake(sm.PeerID, 5*time.Second); err != nil {
		_ = peer.Close()
		return
	}

	sm.registerPeer(peer)
}

// ConnectPeer dials and establishes a session with an outbound peer.
func (sm *SwarmManager) ConnectPeer(address string) (*PeerConnection, error) {
	sm.mu.RLock()
	if _, exists := sm.peers[address]; exists {
		sm.mu.RUnlock()
		return nil, nil // Already connected
	}
	sm.mu.RUnlock()

	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial failed to %s: %w", address, err)
	}

	peer := NewPeerConnection(conn, address, sm.InfoHash, sm.Storage.Manifest.PieceCount)
	if err := peer.PerformHandshake(sm.PeerID, 5*time.Second); err != nil {
		_ = peer.Close()
		return nil, err
	}

	sm.registerPeer(peer)
	return peer, nil
}

// RegisterExistingConnection attaches a pre-established net.Conn (e.g. net.Pipe in tests).
func (sm *SwarmManager) RegisterExistingConnection(conn net.Conn, address string) (*PeerConnection, error) {
	peer := NewPeerConnection(conn, address, sm.InfoHash, sm.Storage.Manifest.PieceCount)
	if err := peer.PerformHandshake(sm.PeerID, 5*time.Second); err != nil {
		_ = peer.Close()
		return nil, err
	}
	sm.registerPeer(peer)
	return peer, nil
}

func (sm *SwarmManager) registerPeer(peer *PeerConnection) {
	sm.mu.Lock()
	sm.peers[peer.Address] = peer
	sm.mu.Unlock()

	// Start reading from peer immediately so socket/pipe buffers never block
	go sm.peerLoop(peer)

	// Send initial wire handshake messages: Bitfield, Unchoke, Interested
	go func() {
		_ = peer.SendMessage(&protocol.Message{
			ID:      protocol.MsgBitfield,
			Payload: sm.Storage.Bitfield.Bytes(),
		})
		_ = peer.SendMessage(&protocol.Message{ID: protocol.MsgUnchoke})
		_ = peer.SendMessage(&protocol.Message{ID: protocol.MsgInterested})
	}()
}

func (sm *SwarmManager) unregisterPeer(peer *PeerConnection) {
	_ = peer.Close()
	sm.mu.Lock()
	delete(sm.peers, peer.Address)
	sm.mu.Unlock()
}

func (sm *SwarmManager) peerLoop(peer *PeerConnection) {
	defer sm.unregisterPeer(peer)

	for {
		if peer.IsClosed() {
			return
		}

		msg, err := peer.ReadMessage(60 * time.Second)
		if err != nil {
			return
		}
		if msg == nil {
			// Keep-alive heartbeat
			continue
		}

		switch msg.ID {
		case protocol.MsgChoke:
			peer.SetChoked(true)
		case protocol.MsgUnchoke:
			peer.SetChoked(false)
		case protocol.MsgInterested:
			peer.SetInterested(true)
		case protocol.MsgNotInterested:
			peer.SetInterested(false)
		case protocol.MsgHave:
			pieceIndex, err := protocol.ParseHave(msg)
			if err == nil {
				peer.SetPiece(pieceIndex)
			}
		case protocol.MsgBitfield:
			peer.SetBitfield(bitfield.FromBytes(msg.Payload, sm.Storage.Manifest.PieceCount))
		case protocol.MsgRequest:
			index, begin, length, err := protocol.ParseRequest(msg)
			if err == nil {
				block, readErr := sm.Storage.ReadBlock(index, begin, length)
				if readErr == nil {
					_ = peer.SendMessage(protocol.FormatPiece(index, begin, block))
				}
			}
		case protocol.MsgPiece:
			index, begin, blockData, err := protocol.ParsePiece(msg)
			if err == nil {
				peer.DispatchBlock(&BlockData{
					Index: index,
					Begin: begin,
					Data:  blockData,
				})
			}
		}
	}
}

// BroadcastHave announces a newly downloaded piece to all connected peers.
func (sm *SwarmManager) BroadcastHave(index int) {
	haveMsg := protocol.FormatHave(index)
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, p := range sm.peers {
		_ = p.SendMessage(haveMsg)
	}
}

// DownloadAll drives the concurrent rarest-first piece downloader.
func (sm *SwarmManager) DownloadAll(ctx context.Context, workers int) error {
	if sm.Storage.Bitfield.IsComplete() {
		sm.signalDone()
		return nil
	}

	if workers <= 0 {
		workers = 4
	}

	var wg sync.WaitGroup
	errChan := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if sm.Storage.Bitfield.IsComplete() {
					sm.signalDone()
					return
				}

				pieceIdx, peer := sm.pickNextPiece()
				if peer == nil {
					// No pieces available or peers choked; sleep briefly before retry
					time.Sleep(100 * time.Millisecond)
					continue
				}

				err := sm.downloadPieceFromPeer(ctx, peer, pieceIdx)
				sm.mu.Lock()
				delete(sm.inFlight, pieceIdx)
				sm.mu.Unlock()

				if err != nil {
					continue
				}

				// Announce completed piece to all swarm members
				sm.BroadcastHave(pieceIdx)
			}
		}()
	}

	wg.Wait()
	close(errChan)

	if sm.Storage.Bitfield.IsComplete() {
		sm.signalDone()
		return nil
	}

	return nil
}

func (sm *SwarmManager) signalDone() {
	sm.closeOnce.Do(func() {
		close(sm.doneChan)
	})
}

// pickNextPiece chooses the rarest piece available among connected unchoked peers.
func (sm *SwarmManager) pickNextPiece() (int, *PeerConnection) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	missing := sm.Storage.Bitfield.MissingPieces()
	if len(missing) == 0 {
		return -1, nil
	}

	type pieceCandidate struct {
		index int
		count int
		peers []*PeerConnection
	}

	var candidates []pieceCandidate

	for _, idx := range missing {
		if _, inFlight := sm.inFlight[idx]; inFlight {
			continue
		}

		var availablePeers []*PeerConnection
		for _, peer := range sm.peers {
			if !peer.IsChoked() && peer.HasPiece(idx) {
				availablePeers = append(availablePeers, peer)
			}
		}

		if len(availablePeers) > 0 {
			candidates = append(candidates, pieceCandidate{
				index: idx,
				count: len(availablePeers),
				peers: availablePeers,
			})
		}
	}

	if len(candidates) == 0 {
		return -1, nil
	}

	// Sort rarest-first (ascending order of availability in swarm)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].count < candidates[j].count
	})

	chosen := candidates[0]
	sm.inFlight[chosen.index] = struct{}{}
	return chosen.index, chosen.peers[0]
}

// downloadPieceFromPeer pipelines block requests to assemble a complete piece.
func (sm *SwarmManager) downloadPieceFromPeer(ctx context.Context, peer *PeerConnection, pieceIdx int) error {
	pieceLen := sm.Storage.Manifest.PieceLength(pieceIdx)
	pieceBuf := make([]byte, pieceLen)
	downloaded := 0

	for downloaded < pieceLen {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		reqSize := BlockSize
		if downloaded+reqSize > pieceLen {
			reqSize = pieceLen - downloaded
		}

		reqMsg := protocol.FormatRequest(pieceIdx, downloaded, reqSize)
		if err := peer.SendMessage(reqMsg); err != nil {
			return err
		}

		// Wait for response block from peer multiplexer
		block, err := peer.WaitForBlock(ctx, 10*time.Second)
		if err != nil {
			return err
		}

		if block.Index != pieceIdx || block.Begin != downloaded {
			return fmt.Errorf("unexpected block payload: got pIdx=%d begin=%d, expected %d/%d",
				block.Index, block.Begin, pieceIdx, downloaded)
		}

		copy(pieceBuf[downloaded:], block.Data)
		downloaded += len(block.Data)
		sm.downloadedBytes.Add(int64(len(block.Data)))
	}

	// Verify and write to storage
	if err := sm.Storage.WritePiece(pieceIdx, pieceBuf); err != nil {
		return fmt.Errorf("piece integrity check failed: %w", err)
	}

	return nil
}

// Close shuts down swarm connections and releases resources.
func (sm *SwarmManager) Close() error {
	sm.mu.Lock()
	if sm.closed {
		sm.mu.Unlock()
		return nil
	}
	sm.closed = true

	if sm.tcpListener != nil {
		_ = sm.tcpListener.Close()
	}

	for _, peer := range sm.peers {
		_ = peer.Close()
	}
	sm.peers = make(map[string]*PeerConnection)
	sm.mu.Unlock()

	return sm.Storage.Close()
}
