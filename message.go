package vertigo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	protocolVersion = uint32(3 << 16)
	sslMagicNumber  = uint32(80877103)
)

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

func (m *Message) Print() {
	fmt.Printf("%s %q\n", string(m.MessageType), m.content.Bytes())
}

func buildMessage(messageType byte) Message {
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

func SSLRequestMessage() Message {
	sslRequestMessage := buildMessage(0)
	sslRequestMessage.Write(sslMagicNumber)
	return sslRequestMessage
}

func StartupMessage(username string, database string) Message {
	startupMessage := buildMessage(0)
	startupMessage.Write(protocolVersion)
	if username != "" {
		startupMessage.WriteString("user")
		startupMessage.WriteString(username)
	}
	if database != "" {
		startupMessage.WriteString("database")
		startupMessage.WriteString(database)
	}

	startupMessage.WriteNull()
	return startupMessage
}

func TerminateMessage() Message {
	return buildMessage('X')
}

func QueryMessage(sql string) Message {
	queryMessage := buildMessage('Q')
	queryMessage.WriteString(sql)
	return queryMessage
}
