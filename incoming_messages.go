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

type ErrorResponseMessage struct{
	Fields map[byte]string
}

func parseErrorResponseMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := ErrorResponseMessage{}
	msg.Fields = make(map[byte]string)
	for {
		var fieldType byte
		if err := decodeNumeric(reader, &fieldType); err != nil {
			return msg, err
		}

		if fieldType == 0 {
			break
		}

		if str, err := decodeString(reader); err != nil {
			return msg, err
		} else {
			msg.Fields[fieldType] = str
		}
	}
	return msg, nil
}

func (msg ErrorResponseMessage) Error() string {
	return fmt.Sprintf("Vertica %s %s: %s", msg.Fields['S'], msg.Fields['C'], msg.Fields['M'])
}


type EmptyQueryMessage struct{}

func parseEmptyQueryMessage(reader *bufio.Reader) (IncomingMessage, error) {
	return EmptyQueryMessage{}, nil
}

func (msg EmptyQueryMessage) Error() string {
	return "The provided SQL string was empty"
}


type AuthenticationRequestMessage struct {
	AuthCode uint32
	Salt     []byte
}

func parseAuthenticationRequestMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := AuthenticationRequestMessage{}
	err := decodeNumeric(reader, &msg.AuthCode)
	return msg, err
}

type ReadyForQueryMessage struct {
	TransactionStatus byte
}

func parseReadyForQueryMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := ReadyForQueryMessage{}
	err := decodeNumeric(reader, &msg.TransactionStatus)
	return msg, err
}

type ParameterStatusMessage struct {
	Name  string
	Value string
}

func parseParameterStatusMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := ParameterStatusMessage{}
	if str, err := decodeString(reader); err != nil {
		return msg, err
	} else {
		msg.Name = str
	}
	if str, err := decodeString(reader); err != nil {
		return msg, err
	} else {
		msg.Value = str
	}
	return msg, nil
}

type BackendKeyDataMessage struct {
	Pid uint32
	Key uint32
}

func parseBackendKeyDataMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := BackendKeyDataMessage{}
	if err := decodeNumeric(reader, &msg.Pid); err != nil {
		return msg, err
	}
	if err := decodeNumeric(reader, &msg.Key); err != nil {
		return msg, err
	}
	return msg, nil
}

type CommandCompleteMessage struct {
	Result string
}

func parseCommandCompleteMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := CommandCompleteMessage{}
	if str, err := decodeString(reader); err != nil {
		return msg, err
	} else {
		msg.Result = str
	}

	return msg, nil
}

type RowDescriptionMessage struct {
	Fields []Field
}

func parseRowDescriptionMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := RowDescriptionMessage{}
	var numFields uint16
	if err := decodeNumeric(reader, &numFields); err != nil {
		return msg, err
	}

	msg.Fields = make([]Field, numFields)
	for i := range msg.Fields {
		field := &msg.Fields[i]

		if name, err := decodeString(reader); err != nil {
			return msg, err
		} else {
			field.Name = name
		}

		if err := decodeNumeric(reader, &field.TableOID); err != nil {
			return msg, err
		}

		if err := decodeNumeric(reader, &field.AttributeNumber); err != nil {
			return msg, err
		}

		if err := decodeNumeric(reader, &field.DataTypeOID); err != nil {
			return msg, err
		}

		if err := decodeNumeric(reader, &field.DataTypeSize); err != nil {
			return msg, err
		}

		if err := decodeNumeric(reader, &field.TypeModifier); err != nil {
			return msg, err
		}

		if err := decodeNumeric(reader, &field.FormatCode); err != nil {
			return msg, err
		}
	}
	return msg, nil
}


type DataRowMessage struct {
	Values []interface{}
}

func parseDataRowMessage(reader *bufio.Reader) (IncomingMessage, error) {
	msg := DataRowMessage{}
	var numValues uint16
	if err := decodeNumeric(reader, &numValues); err != nil {
		return msg, err
	}

	msg.Values = make([]interface{}, numValues)
	for i := range msg.Values {
		var size uint32
		if err := decodeNumeric(reader, &size); err != nil {
			return msg, err
		}

		if size == 0xffffffff {
			msg.Values[i] = nil
		} else {
			value := make([]byte, size)
			if _, err := io.ReadFull(reader, value); err != nil {
				return msg, err
			}
			msg.Values[i] = string(value)
		}
	}

	return msg, nil
}



type messageFactoryMethod func(reader *bufio.Reader) (IncomingMessage, error)

var messageFactoryMethods = map[byte]messageFactoryMethod{
	'R': parseAuthenticationRequestMessage,
	'Z': parseReadyForQueryMessage,
	'E': parseErrorResponseMessage,
	'I': parseEmptyQueryMessage,
	'S': parseParameterStatusMessage,
	'K': parseBackendKeyDataMessage,
	'T': parseRowDescriptionMessage,
	'C': parseCommandCompleteMessage,
	'D': parseDataRowMessage,
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

func decodeNumeric(reader *bufio.Reader, data interface{}) error {
	return binary.Read(reader, binary.BigEndian, data)
}

func decodeString(reader *bufio.Reader) (str string, err error) {
	if str, err = reader.ReadString(0); err != nil {
		return str, err
	}
	return str[0 : len(str)-1], nil
}
