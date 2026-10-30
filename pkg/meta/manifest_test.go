package meta

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateAndLoadManifest(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "testdata.bin")

	// 1 MB test file + 123 bytes (to test uneven chunk)
	dataSize := 1024*1024 + 123
	data := make([]byte, dataSize)
	_, err := rand.Read(data)
	if err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}

	if err := os.WriteFile(testFilePath, data, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	pieceSize := 256 * 1024
	manifest, err := CreateFromFile(testFilePath, pieceSize, "http://localhost:8080/announce")
	if err != nil {
		t.Fatalf("failed to create manifest: %v", err)
	}

	if manifest.FileSize != int64(dataSize) {
		t.Fatalf("expected file size %d, got %d", dataSize, manifest.FileSize)
	}

	expectedPieceCount := 5 // 4 full 256KB pieces + 1 remainder
	if manifest.PieceCount != expectedPieceCount {
		t.Fatalf("expected %d pieces, got %d", expectedPieceCount, manifest.PieceCount)
	}

	if manifest.PieceLength(4) != 123 {
		t.Fatalf("expected last piece length 123, got %d", manifest.PieceLength(4))
	}

	// Verify pieces
	for i := 0; i < manifest.PieceCount; i++ {
		start := i * pieceSize
		end := start + manifest.PieceLength(i)
		chunk := data[start:end]
		if !manifest.VerifyPiece(i, chunk) {
			t.Fatalf("piece %d verification failed", i)
		}
	}

	// Corrupt check
	corruptChunk := make([]byte, manifest.PieceLength(0))
	if manifest.VerifyPiece(0, corruptChunk) {
		t.Fatalf("corrupted chunk should not have passed verification")
	}

	// Save and Load test
	manifestPath := filepath.Join(tmpDir, "test.p2p")
	if err := manifest.Save(manifestPath); err != nil {
		t.Fatalf("failed to save manifest: %v", err)
	}

	loaded, err := Load(manifestPath)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}

	if loaded.InfoHash != manifest.InfoHash {
		t.Fatalf("info hash mismatch: %s vs %s", loaded.InfoHash, manifest.InfoHash)
	}
}
