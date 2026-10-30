package storage

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"p2p/pkg/meta"
)

func TestStorageOutOfOrderAndResume(t *testing.T) {
	tmpDir := t.TempDir()
	sourcePath := filepath.Join(tmpDir, "source.bin")
	destPath := filepath.Join(tmpDir, "downloaded.bin")

	// Create 512KB + 42 bytes test file
	fileSize := 512*1024 + 42
	data := make([]byte, fileSize)
	_, _ = rand.Read(data)
	if err := os.WriteFile(sourcePath, data, 0644); err != nil {
		t.Fatalf("failed to write source: %v", err)
	}

	pieceSize := 256 * 1024
	manifest, err := meta.CreateFromFile(sourcePath, pieceSize, "")
	if err != nil {
		t.Fatalf("failed to create manifest: %v", err)
	}

	tf, err := OpenOrCreate(manifest, destPath)
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}
	defer tf.Close()

	if tf.Bitfield.Count() != 0 {
		t.Fatalf("expected initial bitfield count 0, got %d", tf.Bitfield.Count())
	}

	// Write pieces out of order: Piece 2 (last), then Piece 0
	p2Data := data[2*pieceSize:]
	if err := tf.WritePiece(2, p2Data); err != nil {
		t.Fatalf("failed to write piece 2: %v", err)
	}

	p0Data := data[0:pieceSize]
	if err := tf.WritePiece(0, p0Data); err != nil {
		t.Fatalf("failed to write piece 0: %v", err)
	}

	if tf.Bitfield.Count() != 2 {
		t.Fatalf("expected bitfield count 2, got %d", tf.Bitfield.Count())
	}

	// Verify ReadBlock on Piece 0
	subBlock, err := tf.ReadBlock(0, 10, 100)
	if err != nil {
		t.Fatalf("failed to read block: %v", err)
	}
	if len(subBlock) != 100 {
		t.Fatalf("expected sub-block len 100, got %d", len(subBlock))
	}

	// Attempt to write corrupted piece 1
	p1Corrupt := make([]byte, manifest.PieceLength(1))
	if err := tf.WritePiece(1, p1Corrupt); err == nil {
		t.Fatalf("corrupted piece should have failed verification")
	}

	// Close storage to simulate process termination/restart
	_ = tf.Close()

	// Re-open (Resume test)
	tfResume, err := OpenOrCreate(manifest, destPath)
	if err != nil {
		t.Fatalf("failed to resume storage: %v", err)
	}
	defer tfResume.Close()

	// Bitfield must remember piece 0 and 2
	if !tfResume.Bitfield.HasPiece(0) || !tfResume.Bitfield.HasPiece(2) || tfResume.Bitfield.HasPiece(1) {
		t.Fatalf("resume failed: bitfield state mismatch")
	}

	// Write missing piece 1
	p1Data := data[pieceSize : 2*pieceSize]
	if err := tfResume.WritePiece(1, p1Data); err != nil {
		t.Fatalf("failed to write missing piece 1: %v", err)
	}

	if !tfResume.Bitfield.IsComplete() {
		t.Fatalf("expected file download to be complete")
	}

	_ = tfResume.Close()

	// Compare final downloaded file with source
	downloadedData, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}
	if string(downloadedData) != string(data) {
		t.Fatalf("final downloaded file does not match original byte-for-byte")
	}
}
