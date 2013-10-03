package vertigo

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"fmt"
)

var (
	SslNotSupported                  = errors.New("SSL not available on this server")
	AuthenticationMethodNotSupported = errors.New("Authentication method not supported")
)

type ConnectionInfo struct {
	Address   string
	Username  string
	Database  string
	Password  string
	SslConfig *tls.Config
}

type Connection struct {
	socket     net.Conn
	mutex      sync.Mutex
	parameters map[string]string
	backendPid uint32
	backendKey uint32
}

func Connect(info *ConnectionInfo) (connection *Connection, err error) {
	connection = &Connection{}
	connection.mutex.Lock()
	defer func() {
		if err != nil {
			connection.socket.Close()
		}
		connection.mutex.Unlock()
	}()

	socket, err := net.Dial("tcp", info.Address)
	if err != nil {
		return
	}

	if info.SslConfig != nil {
		sslRequest := SSLRequestMessage()
		sslRequest.Send(socket)

		sslResponse := make([]byte, 1)
		io.ReadFull(socket, sslResponse)
		if sslResponse[0] == byte('S') {
			sslSocket := tls.Client(socket, info.SslConfig)
			if tlsError := sslSocket.Handshake(); tlsError != nil {
				sslSocket.Close()
				return nil, tlsError
			}
			connection.socket = sslSocket
		} else {
			socket.Close()
			return nil, SslNotSupported
		}
	} else {
		connection.socket = socket
	}

	connection.parameters = make(map[string]string)

	if err = connection.initConnection(info); err != nil {
		connection.socket.Close()
	}
	return
}

func (c *Connection) handleStatelessMessage(msg *IncomingMessage) {
	var err error 
	
	switch msg.MessageType {
	case 'S':
		err = c.handleParameterMessage(msg)
	case 'K':
		err = c.handleBackendKeyDataMessage(msg)
	default:
		err = fmt.Errorf("Unexpected message %c", msg.MessageType)
	}

	if err != nil {
		panic(err)
	}
}

func (c *Connection) handleParameterMessage(msg *IncomingMessage) (err error) {
	var (
		key   string
		value string
	)
	
	if key, err = msg.ReadString(); err != nil {
		return 
	}
	
	if value, err = msg.ReadString(); err != nil {
		return
	}
	
	c.parameters[key] = value
	return
}

func (c *Connection) handleBackendKeyDataMessage(msg *IncomingMessage) (err error) {
	if err = msg.Read(&c.backendPid); err != nil {
		return
	}
	if err = msg.Read(&c.backendKey); err != nil {
		return
	}
	return
}

func (c *Connection) initConnection(info *ConnectionInfo) error {

	c.sendMessage(StartupMessage(info.Username, info.Database))
	for msg, err := c.receiveMessage(); msg.MessageType != 'Z'; msg, err = c.receiveMessage() {
		if err != nil {
			return err
		}

		switch msg.MessageType {
		case 'R':
			var authCode uint32
			if readError := msg.Read(&authCode); readError != nil {
				return readError
			}

			switch authCode {
			case AuthenticationOK:
				continue
			case AuthenticationCleartextPassword:
				c.sendMessage(PasswordMessage(info.Password, authCode))
			default:
				return AuthenticationMethodNotSupported
			}

		default:
			c.handleStatelessMessage(msg)
		}
	}
	return nil
}

func (c *Connection) Query(sql string) error {

	if err := c.sendMessage(QueryMessage(sql)); err != nil {
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
	c.sendMessage(TerminateMessage())
	c.socket.Close()
	return nil
}

func (c *Connection) sendMessage(m OutgoingMessage) error {
	m.Print()
	return m.Send(c.socket)
}

func (c *Connection) receiveMessage() (*IncomingMessage, error) {
	msg, err := ReadMessage(c.socket)
	if err != nil {
		return nil, err
	}

	msg.Print()
	return msg, nil
}
