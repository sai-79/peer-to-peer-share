package swarm

import (
	"context"
	"crypto/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"p2p/pkg/meta"
	"p2p/pkg/protocol"
	"p2p/pkg/storage"
)

func TestSwarmEndToEndTransfer(t *testing.T) {
	tmpDir := t.TempDir()
	sourcePath := filepath.Join(tmpDir, "original.dat")
	destPath := filepath.Join(tmpDir, "downloaded.dat")

	// 64 KB test file with 16 KB piece size = 4 pieces
	fileSize := 64 * 1024
	pieceSize := 16 * 1024
	data := make([]byte, fileSize)
	_, _ = rand.Read(data)

	if err := os.WriteFile(sourcePath, data, 0644); err != nil {
		t.Fatalf("failed to write original data: %v", err)
	}

	manifest, err := meta.CreateFromFile(sourcePath, pieceSize, "")
	if err != nil {
		t.Fatalf("failed to create manifest: %v", err)
	}

	// 1. Setup Seeder
	seederStorage, err := storage.OpenOrCreate(manifest, sourcePath)
	if err != nil {
		t.Fatalf("failed to open seeder storage: %v", err)
	}
	defer seederStorage.Close()

	if !seederStorage.Bitfield.IsComplete() {
		t.Fatalf("seeder bitfield should be complete")
	}

	seederPeerID := protocol.GeneratePeerID()
	seederManager, err := NewSwarmManager(seederPeerID, seederStorage)
	if err != nil {
		t.Fatalf("failed to create seeder swarm: %v", err)
	}
	defer seederManager.Close()

	// 2. Setup Leecher
	leecherStorage, err := storage.OpenOrCreate(manifest, destPath)
	if err != nil {
		t.Fatalf("failed to open leecher storage: %v", err)
	}
	defer leecherStorage.Close()

	if leecherStorage.Bitfield.Count() != 0 {
		t.Fatalf("leecher bitfield should start empty")
	}

	leecherPeerID := protocol.GeneratePeerID()
	leecherManager, err := NewSwarmManager(leecherPeerID, leecherStorage)
	if err != nil {
		t.Fatalf("failed to create leecher swarm: %v", err)
	}
	defer leecherManager.Close()

	// 3. Connect them via in-memory net.Pipe()
	leecherSide, seederSide := net.Pipe()

	var registerErr1, registerErr2 error
	var wgHandshake sync.WaitGroup
	wgHandshake.Add(2)

	go func() {
		defer wgHandshake.Done()
		_, registerErr1 = seederManager.RegisterExistingConnection(seederSide, "leecher.pipe")
	}()

	go func() {
		defer wgHandshake.Done()
		_, registerErr2 = leecherManager.RegisterExistingConnection(leecherSide, "seeder.pipe")
	}()

	wgHandshake.Wait()

	if registerErr1 != nil || registerErr2 != nil {
		t.Fatalf("handshake failed: seederErr=%v, leecherErr=%v", registerErr1, registerErr2)
	}

	// Wait for bitfield exchange
	time.Sleep(100 * time.Millisecond)

	// 4. Run download
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = leecherManager.DownloadAll(ctx, 2)
	if err != nil {
		t.Fatalf("download failed: %v", err)
	}

	if !leecherStorage.Bitfield.IsComplete() {
		t.Fatalf("leecher bitfield not complete: %d of %d pieces",
			leecherStorage.Bitfield.Count(), manifest.PieceCount)
	}

	// Verify byte-level equality
	downloadedData, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("failed to read dest file: %v", err)
	}

	if string(downloadedData) != string(data) {
		t.Fatalf("data corruption: downloaded bytes do not match source")
	}
}
