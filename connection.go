package vertigo

import (
	"crypto/tls"
	"io"
	"net"
	"errors"
)

var (
	SslNotSupported = errors.New("SSL not available on this server")
)

type ConnectionInfo struct {
	Address   string
	Username  string
	Password  string
	SslConfig *tls.Config
}

type Connection struct {
	socket net.Conn
}

func Connect(info *ConnectionInfo) (connection *Connection, err error) {
	socket, err := net.Dial("tcp", info.Address)
	if err != nil {
		return nil, err
	}

	if info.SslConfig != nil {
		sslRequest := BuildMessage(0)
		sslRequest.Write(SslMagicNumber)
		sslRequest.Send(socket)

		sslResponse := make([]byte, 1)
		io.ReadFull(socket, sslResponse)
		if sslResponse[0] == byte('S') {
			sslSocket := tls.Client(socket, info.SslConfig)
			if tlsError := sslSocket.Handshake(); tlsError != nil {
				sslSocket.Close()
				return nil, tlsError
			}

			connection = &Connection{sslSocket}
		} else {
			socket.Close()
			return nil, SslNotSupported
		}
	} else {
		connection = &Connection{socket}
	}

	if err := connection.initConnection(info); err != nil {
		socket.Close()
		return nil, err
	}
	return
}

func (c *Connection) initConnection(info *ConnectionInfo) error {
	startupMessage := BuildMessage(0)
	startupMessage.Write(ProtocolVersion)
	startupMessage.WriteString("user")
	startupMessage.WriteString(info.Username)
	startupMessage.WriteNull()
	c.sendMessage(startupMessage)

	for msg, err := c.receiveMessage(); msg.MessageType != 'Z'; msg, err = c.receiveMessage() {
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Connection) Query(sql string) error {
	queryMessage := BuildMessage('Q')
	queryMessage.WriteString(sql)

	if err := c.sendMessage(queryMessage); err != nil {
		return err
	}

	for msg, err := c.receiveMessage(); msg.MessageType != 'Z'; msg, err = c.receiveMessage() {
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Connection) Close() error {
	terminateMessage := BuildMessage('X')
	c.sendMessage(terminateMessage)
	c.socket.Close()
	return nil
}

func (c *Connection) sendMessage(m Message) error {
	m.Print()
	return m.Send(c.socket)
}

func (c *Connection) receiveMessage() (*Message, error) {
	msg, err := ReadMessage(c.socket)
	if err != nil {
		return nil, err
	}

	msg.Print()
	return msg, nil
}
