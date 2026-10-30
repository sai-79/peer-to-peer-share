package discovery

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type inMemoryTransport struct {
	handler http.Handler
}

func (t *inMemoryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	// Simulate RemoteAddr as a client IP
	req.RemoteAddr = "127.0.0.1:54321"
	t.handler.ServeHTTP(rec, req)
	return rec.Result(), nil
}

func TestTrackerLifecycle(t *testing.T) {
	server := NewTrackerServer(8080)
	client := &http.Client{
		Transport: &inMemoryTransport{handler: server.Handler()},
	}

	trackerURL := "http://tracker.local/announce"
	infoHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	// Peer 1 (Seeder: left = 0)
	peer1ID := "PEER_ONE_12345678901"
	resp1, err := AnnounceWithClient(client, trackerURL, infoHash, peer1ID, 6881, 0)
	if err != nil {
		t.Fatalf("peer 1 announce failed: %v", err)
	}
	if resp1.Complete != 1 || resp1.Incomplete != 0 {
		t.Fatalf("expected 1 complete, 0 incomplete; got c=%d, inc=%d", resp1.Complete, resp1.Incomplete)
	}
	if len(resp1.Peers) != 0 {
		t.Fatalf("peer 1 should not see itself in peer list")
	}

	// Peer 2 (Leecher: left = 5000)
	peer2ID := "PEER_TWO_12345678902"
	resp2, err := AnnounceWithClient(client, trackerURL, infoHash, peer2ID, 6882, 5000)
	if err != nil {
		t.Fatalf("peer 2 announce failed: %v", err)
	}
	if resp2.Complete != 1 || resp2.Incomplete != 1 {
		t.Fatalf("expected 1 complete, 1 incomplete; got c=%d, inc=%d", resp2.Complete, resp2.Incomplete)
	}
	if len(resp2.Peers) != 1 {
		t.Fatalf("peer 2 should see peer 1, got %d peers", len(resp2.Peers))
	}
	if resp2.Peers[0].Port != 6881 || resp2.Peers[0].PeerID != peer1ID {
		t.Fatalf("peer info mismatch: %+v", resp2.Peers[0])
	}
}
