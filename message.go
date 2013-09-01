package vertigo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const ProtocolVersion = uint32(3 << 16)

type Message struct {
	MessageType byte
	content     *bytes.Buffer
}

func (m *Message) Send(w io.Writer) error {
	if m.MessageType != 0 {
		binary.Write(w, binary.BigEndian, m.MessageType)
	}
	binary.Write(w, binary.BigEndian, uint32(m.content.Len()+4))
	_, writeErr := w.Write(m.content.Bytes())
	return writeErr
}

func (m *Message) Write(data interface{}) error {
	return binary.Write(m.content, binary.BigEndian, data)
}

func (m *Message) WriteString(s string) {
	m.content.Write([]byte(s))
	m.WriteNull()
}

func (m *Message) WriteNull() {
	m.Write(byte(0))
}

func (m *Message) Printf() {
	fmt.Printf("%s %q\n", string(m.MessageType), m.content.Bytes())
}

func BuildMessage(messageType byte) Message {
	return Message{messageType, new(bytes.Buffer)}
}

func ReadMessage(r io.Reader) (*Message, error) {
	var (
		messageType byte
		messageSize uint32
	)

	if err := binary.Read(r, binary.BigEndian, &messageType); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.BigEndian, &messageSize); err != nil {
		return nil, err
	}

	var messageContent []byte
	if messageSize > 4 {
		messageContent = make([]byte, messageSize-4)
		if _, err := io.ReadFull(r, messageContent); err != nil {
			return nil, err
		}
	} else {
		messageContent = make([]byte, 0)
	}

	return &Message{messageType, bytes.NewBuffer(messageContent)}, nil
}
