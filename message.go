package vertigo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"bufio"
)

const (
	protocolVersion = uint32(3 << 16)
	sslMagicNumber  = uint32(80877103)
)

const (
	AuthenticationOK                = 0
	AuthenticationKerberosV5        = 2
	AuthenticationCleartextPassword = 3
	AuthenticationCryptPassword     = 4
	AuthenticationMD5Password       = 5
	AuthenticationSCAMCredential    = 6
	AuthenticationGSS               = 7
	AuthenticationGSSContinue       = 8
	AuthenticationSSPI              = 9
)

type OutgoingMessage struct {
	MessageType byte
	content     *bytes.Buffer
}

type IncomingMessage struct {
	MessageType byte
	content    []byte
	reader     *bufio.Reader
}

func (m *OutgoingMessage) Send(w io.Writer) error {
	if m.MessageType != 0 {
		binary.Write(w, binary.BigEndian, m.MessageType)
	}
	binary.Write(w, binary.BigEndian, uint32(m.content.Len()+4))
	_, writeErr := w.Write(m.content.Bytes())
	return writeErr
}

func (m *OutgoingMessage) Write(data interface{}) error {
	return binary.Write(m.content, binary.BigEndian, data)
}

func (m *OutgoingMessage) WriteString(s string) {
	m.content.Write([]byte(s))
	m.WriteNull()
}

func (m *OutgoingMessage) WriteNull() {
	m.Write(byte(0))
}

func (m *OutgoingMessage) Print() {
	fmt.Printf("%s %q\n", string(m.MessageType), m.content.Bytes())
}

func buildMessage(messageType byte) OutgoingMessage {
	return OutgoingMessage{messageType, new(bytes.Buffer)}
}

func SSLRequestMessage() OutgoingMessage {
	sslRequestMessage := buildMessage(0)
	sslRequestMessage.Write(sslMagicNumber)
	return sslRequestMessage
}

func StartupMessage(username string, database string) OutgoingMessage {
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

func PasswordMessage(password string, authenticationMethod uint32) OutgoingMessage {
	passwordMessage := buildMessage('p')
	switch authenticationMethod {
	case AuthenticationCleartextPassword:
		passwordMessage.WriteString(password)
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
	queryMessage.WriteString(sql)
	return queryMessage
}


func ReadMessage(r io.Reader) (*IncomingMessage, error) {
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

	msg := &IncomingMessage{MessageType: messageType, content: messageContent}
	msg.reader = bufio.NewReader(bytes.NewBuffer(msg.content))
	return msg, nil
}

func (m *IncomingMessage) Read(data interface{}) error {
	return binary.Read(m.reader, binary.BigEndian, data)
}

func (m *IncomingMessage) ReadString() (string, error) {
	return m.reader.ReadString(0)
}

func (m *IncomingMessage) Print() {
	fmt.Printf("%s %q\n", string(m.MessageType), m.content)
}
