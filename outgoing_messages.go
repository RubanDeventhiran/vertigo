package vertigo

import (
	"bytes"
	"encoding/binary"
	"io"
)

const (
	protocolVersion = uint32(3 << 16)
	sslMagicNumber  = uint32(80877103)
)

type OutgoingMessage interface {
	Encode(buffer *bytes.Buffer) (byte, error)
}

type SSLRequestMessage struct{}

func (m SSLRequestMessage) Encode(buffer *bytes.Buffer) (byte, error) {
	return 0, encodeNumeric(buffer, sslMagicNumber)
}

type StartupMessage struct {
	User     string
	Database string
}

func (m StartupMessage) Encode(buffer *bytes.Buffer) (byte, error) {
	encodeNumeric(buffer, protocolVersion)
	if m.User != "" {
		encodeString(buffer, "user")
		encodeString(buffer, m.User)
	}
	if m.Database != "" {
		encodeString(buffer, "database")
		encodeString(buffer, m.Database)
	}

	return 0, encodeNull(buffer)
}

type PasswordMessage struct {
	AuthenticationMethod uint32
	Password             string
}

func (m PasswordMessage) Encode(buffer *bytes.Buffer) (byte, error) {
	switch m.AuthenticationMethod {
	case AuthenticationCleartextPassword:
		return 'p', encodeString(buffer, m.Password)
	default:
		panic("Cannot create password message for unspported authentication method.")
	}
}

type TerminateMessage struct{}

func (m TerminateMessage) Encode(buffer *bytes.Buffer) (byte, error) {
	return 'X', nil
}

type QueryMessage struct {
	SQL string
}

func (m QueryMessage) Encode(buffer *bytes.Buffer) (byte, error) {
	err := encodeString(buffer, m.SQL)
	return 'Q', err
}

func SendMessage(w io.Writer, m OutgoingMessage) error {
	buffer := new(bytes.Buffer)
	messageType, encodeErr := m.Encode(buffer)
	if encodeErr != nil {
		return encodeErr
	}

	if messageType != 0 {
		binary.Write(w, binary.BigEndian, messageType)
	}
	binary.Write(w, binary.BigEndian, uint32(buffer.Len()+4))
	_, writeErr := w.Write(buffer.Bytes())
	return writeErr

}

func encodeNumeric(buffer *bytes.Buffer, data interface{}) error {
	return binary.Write(buffer, binary.BigEndian, data)
}

func encodeString(buffer *bytes.Buffer, s string) error {
	if _, err := buffer.Write([]byte(s)); err != nil {
		return err
	}
	return encodeNull(buffer)
}

func encodeNull(buffer *bytes.Buffer) error {
	return encodeNumeric(buffer, byte(0))
}
