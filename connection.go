package vertigo

import (
  "net"
)

type Connection struct {
  socket net.Conn
}

func Connect(address string, username string) (*Connection, error) {
  socket, err := net.Dial("tcp", address)
  if err != nil {
    return nil, err
  }

  connection := Connection{socket}
  if err := connection.initConnection(username); err != nil {
    return nil, err
  }

  return &connection, nil
}

func (c *Connection) initConnection(username string) (error) {
  startupMessage := BuildMessage(0)
  startupMessage.Write(ProtocolVersion)
  startupMessage.WriteString("user")
  startupMessage.WriteString(username)
  startupMessage.WriteNull()
  c.sendMessage(startupMessage)

  for {
    nextMessage, err := c.receiveMessage()
    if err != nil {
      return err
    }

    if nextMessage.MessageType == 'Z' {
      return nil
    }
  }
}

func (c *Connection) Query(sql string) (error) {
  queryMessage := BuildMessage('Q')
  queryMessage.WriteString(sql)

  if err := c.sendMessage(queryMessage); err != nil {
    return err
  }

  for {
    nextMessage, err := c.receiveMessage()
    if err != nil {
      return err;
    }

    if nextMessage.MessageType == 'Z' {
      return nil
    }
  }
}

func (c *Connection) Close() (error) {
  terminateMessage := BuildMessage('X')
  c.sendMessage(terminateMessage)
  c.socket.Close()
  return nil
}

func (c *Connection) sendMessage(m Message) (error) {
  m.Printf()
  return m.Send(c.socket)
}

func (c *Connection) receiveMessage() (*Message, error) {
  nextMessage, err := ReadMessage(c.socket)
  if err!= nil {
    return nil, err
  } else {
    nextMessage.Printf()
    return nextMessage, nil  
  }
}
