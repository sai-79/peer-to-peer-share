package bitfield

import (
	"sync"
)

// Bitfield is a thread-safe bit array representing chunk availability.
type Bitfield struct {
	mu      sync.RWMutex
	bits    []byte
	numBits int
}

// New creates a new bitfield of the specified bit count.
func New(numBits int) *Bitfield {
	numBytes := (numBits + 7) / 8
	return &Bitfield{
		bits:    make([]byte, numBytes),
		numBits: numBits,
	}
}

// FromBytes creates a bitfield initialized from a raw byte slice.
func FromBytes(data []byte, numBits int) *Bitfield {
	numBytes := (numBits + 7) / 8
	buf := make([]byte, numBytes)
	copy(buf, data)
	return &Bitfield{
		bits:    buf,
		numBits: numBits,
	}
}

// HasPiece checks if the bit at the given index is set (1).
func (b *Bitfield) HasPiece(index int) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if index < 0 || index >= b.numBits {
		return false
	}
	byteIndex := index / 8
	offset := index % 8
	return (b.bits[byteIndex]>>(7-offset))&1 != 0
}

// SetPiece sets the bit at the given index to 1.
func (b *Bitfield) SetPiece(index int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if index < 0 || index >= b.numBits {
		return
	}
	byteIndex := index / 8
	offset := index % 8
	b.bits[byteIndex] |= 1 << (7 - offset)
}

// ClearPiece sets the bit at the given index to 0.
func (b *Bitfield) ClearPiece(index int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if index < 0 || index >= b.numBits {
		return
	}
	byteIndex := index / 8
	offset := index % 8
	b.bits[byteIndex] &^= 1 << (7 - offset)
}

// Count returns the number of set bits (downloaded pieces).
func (b *Bitfield) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0
	for i := 0; i < b.numBits; i++ {
		byteIndex := i / 8
		offset := i % 8
		if (b.bits[byteIndex]>>(7-offset))&1 != 0 {
			count++
		}
	}
	return count
}

// IsComplete returns true if all bits are set.
func (b *Bitfield) IsComplete() bool {
	return b.Count() == b.numBits
}

// TotalPieces returns total bit count.
func (b *Bitfield) TotalPieces() int {
	return b.numBits
}

// Bytes returns a copy of the underlying byte slice.
func (b *Bitfield) Bytes() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	dup := make([]byte, len(b.bits))
	copy(dup, b.bits)
	return dup
}

// MissingPieces returns indices of all pieces not yet downloaded.
func (b *Bitfield) MissingPieces() []int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var missing []int
	for i := 0; i < b.numBits; i++ {
		byteIndex := i / 8
		offset := i % 8
		if (b.bits[byteIndex]>>(7-offset))&1 == 0 {
			missing = append(missing, i)
		}
	}
	return missing
}
