package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"p2p/pkg/bitfield"
	"p2p/pkg/meta"
)

// TorrentFile manages random-access disk storage for a distributed file.
type TorrentFile struct {
	Manifest *meta.Manifest
	Path     string
	file     *os.File
	Bitfield *bitfield.Bitfield
	mu       sync.RWMutex
}

// OpenOrCreate initializes the target file on disk, pre-allocates size,
// and verifies any existing pieces for seamless download resumption.
func OpenOrCreate(manifest *meta.Manifest, destPath string) (*TorrentFile, error) {
	if manifest == nil {
		return nil, errors.New("manifest cannot be nil")
	}

	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create target directory: %w", err)
	}

	f, err := os.OpenFile(destPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create target file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to stat target file: %w", err)
	}

	// Pre-allocate file size if new or truncated
	if stat.Size() != manifest.FileSize {
		if err := f.Truncate(manifest.FileSize); err != nil {
			f.Close()
			return nil, fmt.Errorf("failed to pre-allocate file size: %w", err)
		}
	}

	tf := &TorrentFile{
		Manifest: manifest,
		Path:     destPath,
		file:     f,
		Bitfield: bitfield.New(manifest.PieceCount),
	}

	// Verify pieces already present on disk (resume capability)
	tf.verifyExistingPieces()

	return tf, nil
}

// verifyExistingPieces scans disk to populate bitfield with verified existing chunks.
func (tf *TorrentFile) verifyExistingPieces() {
	buf := make([]byte, tf.Manifest.PieceSize)

	for i := 0; i < tf.Manifest.PieceCount; i++ {
		pieceLen := tf.Manifest.PieceLength(i)
		chunkBuf := buf[:pieceLen]

		offset := int64(i) * int64(tf.Manifest.PieceSize)
		n, err := tf.file.ReadAt(chunkBuf, offset)
		if err == nil && n == pieceLen {
			if tf.Manifest.VerifyPiece(i, chunkBuf) {
				tf.Bitfield.SetPiece(i)
			}
		}
	}
}

// WritePiece validates SHA-256 integrity and writes the complete piece to disk.
func (tf *TorrentFile) WritePiece(index int, data []byte) error {
	if !tf.Manifest.VerifyPiece(index, data) {
		return fmt.Errorf("checksum mismatch for piece %d", index)
	}

	tf.mu.Lock()
	defer tf.mu.Unlock()

	offset := int64(index) * int64(tf.Manifest.PieceSize)
	n, err := tf.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed writing piece %d to disk: %w", index, err)
	}
	if n != len(data) {
		return fmt.Errorf("short write on piece %d: wrote %d of %d", index, n, len(data))
	}

	tf.Bitfield.SetPiece(index)
	return nil
}

// ReadPiece reads an entire piece from disk if present.
func (tf *TorrentFile) ReadPiece(index int) ([]byte, error) {
	if !tf.Bitfield.HasPiece(index) {
		return nil, fmt.Errorf("piece %d not available locally", index)
	}

	tf.mu.RLock()
	defer tf.mu.RUnlock()

	pieceLen := tf.Manifest.PieceLength(index)
	buf := make([]byte, pieceLen)
	offset := int64(index) * int64(tf.Manifest.PieceSize)

	n, err := tf.file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed reading piece %d: %w", index, err)
	}
	if n != pieceLen {
		return nil, fmt.Errorf("short read on piece %d: read %d of %d", index, n, pieceLen)
	}

	return buf, nil
}

// ReadBlock reads a sub-block of a piece to fulfill a peer's Request message.
func (tf *TorrentFile) ReadBlock(index, begin, length int) ([]byte, error) {
	if !tf.Bitfield.HasPiece(index) {
		return nil, fmt.Errorf("piece %d not available", index)
	}

	tf.mu.RLock()
	defer tf.mu.RUnlock()

	pieceLen := tf.Manifest.PieceLength(index)
	if begin+length > pieceLen {
		return nil, fmt.Errorf("block request exceeds piece bounds (%d+%d > %d)", begin, length, pieceLen)
	}

	buf := make([]byte, length)
	offset := int64(index)*int64(tf.Manifest.PieceSize) + int64(begin)

	n, err := tf.file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed reading block at piece %d: %w", index, err)
	}
	if n != length {
		return nil, fmt.Errorf("short read on block: read %d of %d", n, length)
	}

	return buf, nil
}

// Close flushes data and closes the underlying file handle.
func (tf *TorrentFile) Close() error {
	tf.mu.Lock()
	defer tf.mu.Unlock()

	if tf.file != nil {
		_ = tf.file.Sync()
		err := tf.file.Close()
		tf.file = nil
		return err
	}
	return nil
}
