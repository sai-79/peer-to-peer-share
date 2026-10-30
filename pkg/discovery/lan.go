package discovery

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	DefaultMulticastAddress = "239.255.42.99:9871"
	beaconInterval          = 1 * time.Second
)

// BeaconPayload is the wire packet sent over UDP multicast.
type BeaconPayload struct {
	PeerID     string   `json:"peer_id"`
	Port       int      `json:"port"`
	InfoHashes []string `json:"info_hashes"`
}

// PeerDiscoveryCallback is called whenever a peer is discovered on the LAN.
type PeerDiscoveryCallback func(infoHash, peerAddr, peerID string)

// LANBroadcaster manages local subnet peer discovery via UDP multicast.
type LANBroadcaster struct {
	peerID     string
	tcpPort    int
	mu         sync.RWMutex
	infoHashes map[string]struct{}
	stopChan   chan struct{}
	running    bool
	addr       string
}

// NewLANBroadcaster creates a new local discovery manager.
func NewLANBroadcaster(peerID string, tcpPort int) *LANBroadcaster {
	return &LANBroadcaster{
		peerID:     peerID,
		tcpPort:    tcpPort,
		infoHashes: make(map[string]struct{}),
		stopChan:   make(chan struct{}),
		addr:       DefaultMulticastAddress,
	}
}

// AddInfoHash registers a file hash to be broadcasted to local peers.
func (lb *LANBroadcaster) AddInfoHash(infoHash string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.infoHashes[infoHash] = struct{}{}
}

// RemoveInfoHash unregisters a file hash from local broadcast.
func (lb *LANBroadcaster) RemoveInfoHash(infoHash string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	delete(lb.infoHashes, infoHash)
}

// Start begins advertising presence and listening for peer beacons.
func (lb *LANBroadcaster) Start(callback PeerDiscoveryCallback) error {
	lb.mu.Lock()
	if lb.running {
		lb.mu.Unlock()
		return nil
	}
	lb.running = true
	lb.stopChan = make(chan struct{})
	lb.mu.Unlock()

	mAddr, err := net.ResolveUDPAddr("udp", lb.addr)
	if err != nil {
		return fmt.Errorf("failed to resolve multicast addr: %w", err)
	}

	// Listener connection
	conn, err := net.ListenMulticastUDP("udp", nil, mAddr)
	if err != nil {
		// Fallback to standard UDP listener if multicast bind fails
		conn, err = net.ListenUDP("udp", &net.UDPAddr{Port: mAddr.Port})
		if err != nil {
			return fmt.Errorf("failed to bind UDP discovery listener: %w", err)
		}
	}

	// Start receiving
	go lb.listenLoop(conn, callback)

	// Start sending
	go lb.broadcastLoop(mAddr)

	return nil
}

func (lb *LANBroadcaster) listenLoop(conn *net.UDPConn, callback PeerDiscoveryCallback) {
	defer conn.Close()
	buf := make([]byte, 4096)

	for {
		select {
		case <-lb.stopChan:
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		var payload BeaconPayload
		if err := json.Unmarshal(buf[:n], &payload); err != nil {
			continue
		}

		// Ignore own beacons
		if payload.PeerID == lb.peerID {
			continue
		}

		peerIP := src.IP.String()
		peerAddr := net.JoinHostPort(peerIP, fmt.Sprintf("%d", payload.Port))

		if callback != nil {
			for _, hash := range payload.InfoHashes {
				callback(hash, peerAddr, payload.PeerID)
			}
		}
	}
}

func (lb *LANBroadcaster) broadcastLoop(mAddr *net.UDPAddr) {
	conn, err := net.DialUDP("udp", nil, mAddr)
	if err != nil {
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(beaconInterval)
	defer ticker.Stop()

	for {
		select {
		case <-lb.stopChan:
			return
		case <-ticker.C:
			lb.mu.RLock()
			if len(lb.infoHashes) == 0 {
				lb.mu.RUnlock()
				continue
			}

			hashes := make([]string, 0, len(lb.infoHashes))
			for h := range lb.infoHashes {
				hashes = append(hashes, h)
			}
			lb.mu.RUnlock()

			payload := BeaconPayload{
				PeerID:     lb.peerID,
				Port:       lb.tcpPort,
				InfoHashes: hashes,
			}

			data, err := json.Marshal(payload)
			if err == nil {
				_, _ = conn.Write(data)
			}
		}
	}
}

// Stop halts the LAN discovery broadcaster and listener.
func (lb *LANBroadcaster) Stop() {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if !lb.running {
		return
	}
	lb.running = false
	close(lb.stopChan)
}
