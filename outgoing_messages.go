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

type OutgoingMessage struct {
	MessageType byte
	content     *bytes.Buffer
}

func (m *OutgoingMessage) Send(w io.Writer) error {
	m.Log()

	if m.MessageType != 0 {
		binary.Write(w, binary.BigEndian, m.MessageType)
	}
	binary.Write(w, binary.BigEndian, uint32(m.content.Len()+4))
	_, writeErr := w.Write(m.content.Bytes())
	return writeErr
}

func (m *OutgoingMessage) Log() {
	if TrafficLogger != nil {
		TrafficLogger.Printf("=> %s %q\n", string(m.MessageType), m.content.Bytes())
	}
}

func buildMessage(messageType byte) OutgoingMessage {
	return OutgoingMessage{messageType, new(bytes.Buffer)}
}

func SSLRequestMessage() OutgoingMessage {
	sslRequestMessage := buildMessage(0)
	sslRequestMessage.write(sslMagicNumber)
	return sslRequestMessage
}

func StartupMessage(username string, database string) OutgoingMessage {
	startupMessage := buildMessage(0)
	startupMessage.write(protocolVersion)
	if username != "" {
		startupMessage.writeString("user")
		startupMessage.writeString(username)
	}
	if database != "" {
		startupMessage.writeString("database")
		startupMessage.writeString(database)
	}

	startupMessage.writeNull()
	return startupMessage
}

func PasswordMessage(password string, authenticationMethod uint32) OutgoingMessage {
	passwordMessage := buildMessage('p')
	switch authenticationMethod {
	case AuthenticationCleartextPassword:
		passwordMessage.writeString(password)
	default:
		panic("Cannot create password message for unspported authentication method.")
	}
	return passwordMessage
}

func TerminateMessage() OutgoingMessage {
	return buildMessage('X')
}

func QueryMessage(sql string) OutgoingMessage {
	queryMessage := buildMessage('Q')
	queryMessage.writeString(sql)
	return queryMessage
}


func (m *OutgoingMessage) write(data interface{}) error {
	return binary.Write(m.content, binary.BigEndian, data)
}

func (m *OutgoingMessage) writeString(s string) {
	m.content.Write([]byte(s))
	m.writeNull()
}

func (m *OutgoingMessage) writeNull() {
	m.write(byte(0))
}
