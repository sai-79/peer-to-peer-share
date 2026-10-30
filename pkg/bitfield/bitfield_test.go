package bitfield

import (
	"sync"
	"testing"
)

func TestBitfieldBasic(t *testing.T) {
	bf := New(10)
	if bf.Count() != 0 {
		t.Fatalf("expected initial count 0, got %d", bf.Count())
	}
	if bf.IsComplete() {
		t.Fatalf("expected incomplete bitfield")
	}

	bf.SetPiece(0)
	bf.SetPiece(5)
	bf.SetPiece(9)

	if !bf.HasPiece(0) || !bf.HasPiece(5) || !bf.HasPiece(9) {
		t.Fatalf("pieces not set properly")
	}
	if bf.HasPiece(1) || bf.HasPiece(8) {
		t.Fatalf("unexpected piece found set")
	}
	if bf.Count() != 3 {
		t.Fatalf("expected count 3, got %d", bf.Count())
	}

	missing := bf.MissingPieces()
	if len(missing) != 7 {
		t.Fatalf("expected 7 missing pieces, got %d", len(missing))
	}

	bf.ClearPiece(5)
	if bf.HasPiece(5) {
		t.Fatalf("piece 5 should have been cleared")
	}
	if bf.Count() != 2 {
		t.Fatalf("expected count 2, got %d", bf.Count())
	}
}

func TestBitfieldSerialization(t *testing.T) {
	bf := New(16)
	for i := 0; i < 16; i++ {
		if i%2 == 0 {
			bf.SetPiece(i)
		}
	}

	bytes := bf.Bytes()
	bf2 := FromBytes(bytes, 16)

	for i := 0; i < 16; i++ {
		if bf.HasPiece(i) != bf2.HasPiece(i) {
			t.Fatalf("mismatch at index %d: expected %v, got %v", i, bf.HasPiece(i), bf2.HasPiece(i))
		}
	}
}

func TestBitfieldConcurrency(t *testing.T) {
	bf := New(100)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bf.SetPiece(idx)
			_ = bf.HasPiece(idx)
			_ = bf.Count()
		}(i)
	}

	wg.Wait()
	if bf.Count() != 50 {
		t.Fatalf("expected count 50, got %d", bf.Count())
	}
}
