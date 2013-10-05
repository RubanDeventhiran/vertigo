package vertigo

import (
  "bufio"
  "io"
  "encoding/binary"
  "errors"
  "bytes"
)

type IncomingMessage struct {
  MessageType byte
  content     []byte
  reader      *bufio.Reader
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
  if messageSize >= 4 {
    messageContent = make([]byte, messageSize-4)
    if messageSize > 4 {
      if _, err := io.ReadFull(r, messageContent); err != nil {
        return nil, err
      }
    }
  } else {
    return nil, errors.New("A message should be at least 4 bytes long")
  }

  msg := &IncomingMessage{MessageType: messageType, content: messageContent}
  msg.reader = bufio.NewReader(bytes.NewBuffer(msg.content))

  msg.Log()
  return msg, nil
}



func (m *IncomingMessage) Read(data interface{}) error {
  return binary.Read(m.reader, binary.BigEndian, data)
}

func (m *IncomingMessage) ReadString() (string, error) {
  return m.reader.ReadString(0)
}

func (m *IncomingMessage) Log() {
  if TrafficLogger != nil {
    TrafficLogger.Printf("<= %s %q\n", string(m.MessageType), m.content)
  }
}
