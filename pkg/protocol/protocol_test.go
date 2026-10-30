package protocol

import (
	"bytes"
	"reflect"
	"testing"
)

func TestHandshakeSerializeDeserialize(t *testing.T) {
	peerID := GeneratePeerID()
	hashStr := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	infoHash, err := InfoHashFromHex(hashStr)
	if err != nil {
		t.Fatalf("failed to parse infohash: %v", err)
	}

	hs := &Handshake{
		Pstr:     ProtocolIdentifier,
		InfoHash: infoHash,
		PeerID:   peerID,
	}

	buf := hs.Serialize()
	reader := bytes.NewReader(buf)

	parsed, err := ReadHandshake(reader)
	if err != nil {
		t.Fatalf("failed to read handshake: %v", err)
	}

	if parsed.Pstr != ProtocolIdentifier {
		t.Fatalf("expected pstr %s, got %s", ProtocolIdentifier, parsed.Pstr)
	}
	if parsed.InfoHash != infoHash {
		t.Fatalf("info hash mismatch")
	}
	if parsed.PeerID != peerID {
		t.Fatalf("peer id mismatch")
	}
	if !hs.Matches(parsed) {
		t.Fatalf("expected matches to be true")
	}
}

func TestMessageSerialization(t *testing.T) {
	// Test Have
	haveMsg := FormatHave(42)
	serialized := haveMsg.Serialize()
	parsed, err := ReadMessage(bytes.NewReader(serialized))
	if err != nil {
		t.Fatalf("failed to read have message: %v", err)
	}
	idx, err := ParseHave(parsed)
	if err != nil || idx != 42 {
		t.Fatalf("expected index 42, got %d, err: %v", idx, err)
	}

	// Test Request
	reqMsg := FormatRequest(10, 0, 16384)
	parsedReq, err := ReadMessage(bytes.NewReader(reqMsg.Serialize()))
	if err != nil {
		t.Fatalf("failed to read req message: %v", err)
	}
	rIdx, rBegin, rLen, err := ParseRequest(parsedReq)
	if err != nil || rIdx != 10 || rBegin != 0 || rLen != 16384 {
		t.Fatalf("request mismatch: idx=%d, begin=%d, len=%d", rIdx, rBegin, rLen)
	}

	// Test Piece
	payloadData := []byte("hello world p2p chunk data")
	pieceMsg := FormatPiece(7, 1024, payloadData)
	parsedPiece, err := ReadMessage(bytes.NewReader(pieceMsg.Serialize()))
	if err != nil {
		t.Fatalf("failed to read piece message: %v", err)
	}
	pIdx, pBegin, pBlock, err := ParsePiece(parsedPiece)
	if err != nil || pIdx != 7 || pBegin != 1024 || !reflect.DeepEqual(pBlock, payloadData) {
		t.Fatalf("piece mismatch: idx=%d, begin=%d, data=%s", pIdx, pBegin, string(pBlock))
	}

	// Test Keep-Alive (nil)
	var keepAlive *Message
	kaBytes := keepAlive.Serialize()
	parsedKA, err := ReadMessage(bytes.NewReader(kaBytes))
	if err != nil {
		t.Fatalf("unexpected error reading keep alive: %v", err)
	}
	if parsedKA != nil {
		t.Fatalf("expected nil for keepalive, got %+v", parsedKA)
	}
}
