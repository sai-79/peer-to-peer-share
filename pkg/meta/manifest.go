package meta

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// DefaultPieceSize is 256 KB.
const DefaultPieceSize = 256 * 1024

// Manifest defines the file distribution metadata.
type Manifest struct {
	FileName    string    `json:"file_name"`
	FileSize    int64     `json:"file_size"`
	PieceSize   int       `json:"piece_size"`
	PieceCount  int       `json:"piece_count"`
	PieceHashes []string  `json:"piece_hashes"`
	InfoHash    string    `json:"info_hash"`
	TrackerURL  string    `json:"tracker_url,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// CreateFromFile chunks a local file and generates a Manifest.
func CreateFromFile(filePath string, pieceSize int, trackerURL string) (*Manifest, error) {
	if pieceSize <= 0 {
		pieceSize = DefaultPieceSize
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open source file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	fileSize := stat.Size()
	if fileSize == 0 {
		return nil, errors.New("cannot create manifest for empty file")
	}

	pieceCount := int((fileSize + int64(pieceSize) - 1) / int64(pieceSize))
	pieceHashes := make([]string, 0, pieceCount)

	buffer := make([]byte, pieceSize)
	masterHasher := sha256.New()

	for {
		n, readErr := file.Read(buffer)
		if n > 0 {
			chunkHasher := sha256.New()
			chunkHasher.Write(buffer[:n])
			sum := chunkHasher.Sum(nil)
			pieceHashes = append(pieceHashes, hex.EncodeToString(sum))
			masterHasher.Write(sum)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, fmt.Errorf("error reading chunk: %w", readErr)
		}
	}

	infoHash := hex.EncodeToString(masterHasher.Sum(nil))

	return &Manifest{
		FileName:    filepath.Base(filePath),
		FileSize:    fileSize,
		PieceSize:   pieceSize,
		PieceCount:  len(pieceHashes),
		PieceHashes: pieceHashes,
		InfoHash:    infoHash,
		TrackerURL:  trackerURL,
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// Save writes the manifest formatted as JSON to target file.
func (m *Manifest) Save(destPath string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}
	return os.WriteFile(destPath, data, 0644)
}

// Load reads and parses a JSON manifest file.
func Load(srcPath string) (*Manifest, error) {
	data, err := os.ReadFile(srcPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest file: %w", err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("invalid manifest JSON: %w", err)
	}

	if m.FileSize <= 0 || m.PieceSize <= 0 || len(m.PieceHashes) == 0 || m.InfoHash == "" {
		return nil, errors.New("malformed manifest: missing required fields")
	}

	return &m, nil
}

// PieceLength returns expected byte length of piece at index.
func (m *Manifest) PieceLength(index int) int {
	if index < 0 || index >= m.PieceCount {
		return 0
	}
	if index == m.PieceCount-1 {
		remainder := int(m.FileSize % int64(m.PieceSize))
		if remainder != 0 {
			return remainder
		}
	}
	return m.PieceSize
}

// VerifyPiece verifies whether chunk bytes match the manifest's piece hash.
func (m *Manifest) VerifyPiece(index int, data []byte) bool {
	if index < 0 || index >= len(m.PieceHashes) {
		return false
	}
	expectedLen := m.PieceLength(index)
	if len(data) != expectedLen {
		return false
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]) == m.PieceHashes[index]
}
