package vertigo

import (
  "net"
  "crypto/tls"
  "fmt"
)

type ConnectionInfo struct {
  Address  string
  Username string
  Password string
  SslConfig *tls.Config
}

type Connection struct {
  socket net.Conn
}

func Connect(info *ConnectionInfo) (*Connection, error) {
  socket, err := net.Dial("tcp", info.Address)
  if err != nil {
    return nil, err
  }

  var connection Connection

  if info.SslConfig != nil {
    sslRequest := BuildMessage(0)
    sslRequest.Write(uint32(80877103))
    sslRequest.Send(socket)

    sslResponse := make([]byte, 1)
    socket.Read(sslResponse)
    if sslResponse[0] == byte('S') {
      sslSocket := tls.Client(socket, info.SslConfig)
      if tlsError := sslSocket.Handshake(); tlsError != nil {
        sslSocket.Close()
        return nil, tlsError
      }

      connection = Connection{sslSocket}
    } else {
      socket.Close()
      return nil, fmt.Errorf("SSL not available on this server")
    }
  } else {
    connection = Connection{socket}
  }

  
  if err := connection.initConnection(info); err != nil {
    socket.Close()
    return nil, err
  }

  return &connection, nil
}

func (c *Connection) initConnection(info *ConnectionInfo) (error) {
  startupMessage := BuildMessage(0)
  startupMessage.Write(ProtocolVersion)
  startupMessage.WriteString("user")
  startupMessage.WriteString(info.Username)
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
