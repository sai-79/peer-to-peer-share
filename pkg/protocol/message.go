package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MessageID identifies the wire protocol message type.
type MessageID byte

const (
	MsgChoke         MessageID = 0
	MsgUnchoke       MessageID = 1
	MsgInterested    MessageID = 2
	MsgNotInterested MessageID = 3
	MsgHave          MessageID = 4
	MsgBitfield      MessageID = 5
	MsgRequest       MessageID = 6
	MsgPiece         MessageID = 7
	MsgCancel        MessageID = 8
)

// Message is a framed wire protocol message.
type Message struct {
	ID      MessageID
	Payload []byte
}

// Serialize formats the message as [4 bytes length][1 byte ID][Payload].
func (m *Message) Serialize() []byte {
	if m == nil {
		// Keep-alive message: 4 zero bytes
		return make([]byte, 4)
	}

	length := uint32(len(m.Payload) + 1)
	buf := make([]byte, 4+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = byte(m.ID)
	copy(buf[5:], m.Payload)
	return buf
}

// ReadMessage parses the next framed message from an io.Reader.
func ReadMessage(r io.Reader) (*Message, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lengthBuf)
	if length == 0 {
		// Keep-alive
		return nil, nil
	}

	// Safety check against oversized messages (e.g., > 16MB)
	if length > 16*1024*1024 {
		return nil, fmt.Errorf("message length %d exceeds 16MB limit", length)
	}

	messageBuf := make([]byte, length)
	if _, err := io.ReadFull(r, messageBuf); err != nil {
		return nil, err
	}

	return &Message{
		ID:      MessageID(messageBuf[0]),
		Payload: messageBuf[1:],
	}, nil
}

// FormatHave creates a Have message announcing a completed piece.
func FormatHave(index int) *Message {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(index))
	return &Message{ID: MsgHave, Payload: payload}
}

// ParseHave extracts the piece index from a Have message.
func ParseHave(msg *Message) (int, error) {
	if msg.ID != MsgHave {
		return 0, fmt.Errorf("expected MsgHave (%d), got %d", MsgHave, msg.ID)
	}
	if len(msg.Payload) != 4 {
		return 0, fmt.Errorf("expected payload length 4, got %d", len(msg.Payload))
	}
	return int(binary.BigEndian.Uint32(msg.Payload)), nil
}

// FormatRequest creates a piece block request.
func FormatRequest(index, begin, length int) *Message {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	return &Message{ID: MsgRequest, Payload: payload}
}

// ParseRequest extracts index, begin, length from a Request message.
func ParseRequest(msg *Message) (index, begin, length int, err error) {
	if msg.ID != MsgRequest {
		return 0, 0, 0, fmt.Errorf("expected MsgRequest (%d), got %d", MsgRequest, msg.ID)
	}
	if len(msg.Payload) != 12 {
		return 0, 0, 0, fmt.Errorf("expected payload length 12, got %d", len(msg.Payload))
	}
	index = int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	begin = int(binary.BigEndian.Uint32(msg.Payload[4:8]))
	length = int(binary.BigEndian.Uint32(msg.Payload[8:12]))
	return index, begin, length, nil
}

// FormatPiece creates a Piece response message containing chunk data.
func FormatPiece(index, begin int, block []byte) *Message {
	payload := make([]byte, 8+len(block))
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	copy(payload[8:], block)
	return &Message{ID: MsgPiece, Payload: payload}
}

// ParsePiece extracts index, begin, and data block from a Piece message.
func ParsePiece(msg *Message) (index, begin int, block []byte, err error) {
	if msg.ID != MsgPiece {
		return 0, 0, nil, fmt.Errorf("expected MsgPiece (%d), got %d", MsgPiece, msg.ID)
	}
	if len(msg.Payload) < 8 {
		return 0, 0, nil, fmt.Errorf("payload length %d too short for piece header", len(msg.Payload))
	}
	index = int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	begin = int(binary.BigEndian.Uint32(msg.Payload[4:8]))
	block = msg.Payload[8:]
	return index, begin, block, nil
}
