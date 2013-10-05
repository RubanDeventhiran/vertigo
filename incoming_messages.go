package vertigo

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type IncomingMessage interface{}

type AuthenticationRequestMessage struct {
	AuthCode uint32
	Salt     []byte
}

type ReadyForQueryMessage struct {
	TransactionStatus byte
}

type ErrorResponseMessage struct{}
type EmptyQueryMessage struct{}

type ParameterStatusMessage struct {
	Name  string
	Value string
}

type BackendKeyDataMessage struct {
	Pid uint32
	Key uint32
}

func parseErrorResponseMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := ErrorResponseMessage{}
	return msg, nil
}

func parseEmptyQueryMessage(reader *bufio.Reader) (IncomingMessage, error) {
	return EmptyQueryMessage{}, nil
}

func parseAuthenticationRequestMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := AuthenticationRequestMessage{}
	err := readNumeric(reader, &msg.AuthCode)
	return msg, err
}

func parseReadyForQueryMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := ReadyForQueryMessage{}
	err := readNumeric(reader, &msg.TransactionStatus)
	return msg, err
}

func parseParameterStatusMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := ParameterStatusMessage{}
	if str, err := readString(reader); err != nil {
		return msg, err
	} else {
		msg.Name = str
	}
	if str, err := readString(reader); err != nil {
		return msg, err
	} else {
		msg.Value = str
	}
	return msg, nil
}

func parseBackendKeyDataMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := BackendKeyDataMessage{}
	if err := readNumeric(reader, &msg.Pid); err != nil {
		return msg, err
	}
	if err := readNumeric(reader, &msg.Key); err != nil {
		return msg, err
	}
	return msg, nil
}

func (msg ErrorResponseMessage) Error() string {
	return "The server responded with an error"
}

type messageFactoryMethod func(reader *bufio.Reader) (IncomingMessage, error)

var messageFactoryMethods = map[byte]messageFactoryMethod{
	'R': parseAuthenticationRequestMessage,
	'Z': parseReadyForQueryMessage,
	'E': parseErrorResponseMessage,
	'I': parseEmptyQueryMessage,
	'S': parseParameterStatusMessage,
	'K': parseBackendKeyDataMessage,
}

func ReadMessage(r io.Reader) (message IncomingMessage, err error) {
	var (
		messageType byte
		messageSize uint32
	)

	if err = binary.Read(r, binary.BigEndian, &messageType); err != nil {
		return
	}

	if err = binary.Read(r, binary.BigEndian, &messageSize); err != nil {
		return
	}

	var messageContent []byte
	if messageSize >= 4 {
		messageContent = make([]byte, messageSize-4)
		if messageSize > 4 {
			if _, err = io.ReadFull(r, messageContent); err != nil {
				return
			}
		}
	} else {
		err = errors.New("A message should be at least 4 bytes long")
		return
	}

	reader := bufio.NewReader(bytes.NewBuffer(messageContent))
	factoryMethod := messageFactoryMethods[messageType]
	if factoryMethod == nil {
		panic(fmt.Sprintf("Unknown message type: %c", messageType))
	}
	return factoryMethod(reader)
}

func readNumeric(reader *bufio.Reader, data interface{}) error {
	return binary.Read(reader, binary.BigEndian, data)
}

func readString(reader *bufio.Reader) (str string, err error) {
	if str, err = reader.ReadString(0); err != nil {
		return str, err
	}
	return str[0 : len(str)-1], nil

}
