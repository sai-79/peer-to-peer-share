package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// PeerInfo represents a discovered peer candidate.
type PeerInfo struct {
	PeerID string `json:"peer_id"`
	IP     string `json:"ip"`
	Port   int    `json:"port"`
}

func (p PeerInfo) Address() string {
	return net.JoinHostPort(p.IP, strconv.Itoa(p.Port))
}

// AnnounceResponse is returned by the tracker to announcing peers.
type AnnounceResponse struct {
	Interval   int        `json:"interval"`   // in seconds
	Complete   int        `json:"complete"`   // seeders count
	Incomplete int        `json:"incomplete"` // leechers count
	Peers      []PeerInfo `json:"peers"`
}

// TrackerServer manages peer registrations per info-hash.
type TrackerServer struct {
	mu      sync.RWMutex
	swarms  map[string]map[string]peerEntry
	server  *http.Server
	port    int
	stopped chan struct{}
}

type peerEntry struct {
	info     PeerInfo
	left     int64
	lastSeen time.Time
}

// NewTrackerServer creates an embedded HTTP tracker.
func NewTrackerServer(port int) *TrackerServer {
	return &TrackerServer{
		swarms:  make(map[string]map[string]peerEntry),
		port:    port,
		stopped: make(chan struct{}),
	}
}

// Handler returns the tracker's HTTP request handler.
func (ts *TrackerServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/announce", ts.handleAnnounce)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return mux
}

// Start launches the HTTP tracker server on the specified port.
func (ts *TrackerServer) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", ts.port))
	if err != nil {
		return fmt.Errorf("failed to bind tracker port %d: %w", ts.port, err)
	}

	ts.server = &http.Server{
		Handler:      ts.Handler(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		_ = ts.server.Serve(listener)
	}()

	// Periodic stale peer cleanup
	go ts.cleanerLoop()

	return nil
}

// Port returns the actual listening port.
func (ts *TrackerServer) Port() int {
	return ts.port
}

// Stop terminates the tracker HTTP server.
func (ts *TrackerServer) Stop() error {
	close(ts.stopped)
	if ts.server != nil {
		return ts.server.Close()
	}
	return nil
}

func (ts *TrackerServer) handleAnnounce(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	infoHash := q.Get("info_hash")
	peerID := q.Get("peer_id")
	portStr := q.Get("port")
	leftStr := q.Get("left")

	if infoHash == "" || peerID == "" || portStr == "" {
		http.Error(w, "missing required parameters: info_hash, peer_id, port", http.StatusBadRequest)
		return
	}

	peerPort, err := strconv.Atoi(portStr)
	if err != nil || peerPort <= 0 || peerPort > 65535 {
		http.Error(w, "invalid port parameter", http.StatusBadRequest)
		return
	}

	leftBytes, _ := strconv.ParseInt(leftStr, 10, 64)

	// Determine peer IP (check X-Forwarded-For or RemoteAddr)
	clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		clientIP = r.RemoteAddr
	}
	if clientIP == "::1" || clientIP == "" {
		clientIP = "127.0.0.1"
	}

	peerKey := fmt.Sprintf("%s:%d", clientIP, peerPort)

	ts.mu.Lock()
	swarm, exists := ts.swarms[infoHash]
	if !exists {
		swarm = make(map[string]peerEntry)
		ts.swarms[infoHash] = swarm
	}

	swarm[peerKey] = peerEntry{
		info: PeerInfo{
			PeerID: peerID,
			IP:     clientIP,
			Port:   peerPort,
		},
		left:     leftBytes,
		lastSeen: time.Now(),
	}

	complete := 0
	incomplete := 0
	var peersList []PeerInfo

	for k, entry := range swarm {
		if entry.left == 0 {
			complete++
		} else {
			incomplete++
		}
		// Don't send peer to itself
		if k != peerKey {
			peersList = append(peersList, entry.info)
		}
	}
	ts.mu.Unlock()

	resp := AnnounceResponse{
		Interval:   15,
		Complete:   complete,
		Incomplete: incomplete,
		Peers:      peersList,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (ts *TrackerServer) cleanerLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ts.stopped:
			return
		case <-ticker.C:
			ts.mu.Lock()
			now := time.Now()
			for infoHash, swarm := range ts.swarms {
				for key, entry := range swarm {
					if now.Sub(entry.lastSeen) > 2*time.Minute {
						delete(swarm, key)
					}
				}
				if len(swarm) == 0 {
					delete(ts.swarms, infoHash)
				}
			}
			ts.mu.Unlock()
		}
	}
}

// Announce sends an announce request to a tracker URL and returns active peers.
func Announce(trackerURL, infoHash, peerID string, port int, left int64) (*AnnounceResponse, error) {
	return AnnounceWithClient(&http.Client{Timeout: 5 * time.Second}, trackerURL, infoHash, peerID, port, left)
}

// AnnounceWithClient executes an announce using a supplied http.Client.
func AnnounceWithClient(client *http.Client, trackerURL, infoHash, peerID string, port int, left int64) (*AnnounceResponse, error) {
	if trackerURL == "" {
		return nil, errors.New("empty tracker URL")
	}

	reqURL := fmt.Sprintf("%s?info_hash=%s&peer_id=%s&port=%d&left=%d",
		trackerURL, infoHash, peerID, port, left)

	resp, err := client.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("tracker announce failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tracker responded with status %d", resp.StatusCode)
	}

	var announceResp AnnounceResponse
	if err := json.NewDecoder(resp.Body).Decode(&announceResp); err != nil {
		return nil, fmt.Errorf("failed to decode tracker response: %w", err)
	}

	return &announceResp, nil
}
