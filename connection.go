package vertigo

import (
	"io"
	"net"
	"crypto/tls"
	"sync"
	"fmt"
	"errors"
)

var (
	SslNotSupported                  = errors.New("SSL not available on this server")
	AuthenticationMethodNotSupported = errors.New("Authentication method not supported")
	AuthenticationFailed             = errors.New("Authentication failed")
	EmptyQuery                       = errors.New("The provided SQL string was empty")
)

type ConnectionInfo struct {
	Address   string
	Username  string
	Database  string
	Password  string
	SslConfig *tls.Config
}

type Connection struct {
	l sync.Mutex

	socket     net.Conn
	parameters map[string]string
	backendPid uint32
	backendKey uint32
}

func Connect(info *ConnectionInfo) (connection Connection, err error) {
	connection = Connection{}
	defer func() {
		if err != nil {
			connection.resetConnection()
		}
	}()

	connection.l.Lock()
	defer connection.l.Unlock()

	connection.socket, err = net.Dial("tcp", info.Address)
	if err != nil {
		return
	}

	if info.SslConfig != nil {
		connection.sendMessage(SSLRequestMessage())

		sslResponse := make([]byte, 1)
		io.ReadFull(connection.socket, sslResponse)
		if sslResponse[0] == byte('S') {
			tlsSocket := tls.Client(connection.socket, info.SslConfig)
			connection.socket = tlsSocket
			if err = tlsSocket.Handshake(); err != nil {
				return
			}
		} else {
			err = SslNotSupported
			return
		}
	}

	connection.parameters = make(map[string]string)
	return connection, connection.initConnection(info)
}

func (c *Connection) handleStatelessMessage(msg *IncomingMessage) error {
	switch msg.MessageType {
	case 'S':
		return c.handleParameterMessage(msg)
	case 'K':
		return c.handleBackendKeyDataMessage(msg)
	default:
		panic(fmt.Sprintf("Unexpected message %c", msg.MessageType))
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

func (c *Connection) handleBackendKeyDataMessage(msg *IncomingMessage) error {
	if readErr := msg.Read(&c.backendPid); readErr != nil {
		return readErr
	}
	return msg.Read(&c.backendKey)
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

		case 'E':
			// TODO: parse error message
			return AuthenticationFailed

		default:
			c.handleStatelessMessage(msg)
		}
	}
	return nil
}

func (c *Connection) Query(sql string) ([][]interface{}, error) {
	c.l.Lock()
	defer c.l.Unlock()

	if err := c.sendMessage(QueryMessage(sql)); err != nil {
		return nil, err
	}

	var queryError error
	for msg, err := c.receiveMessage(); msg.MessageType != 'Z'; msg, err = c.receiveMessage() {
		if err != nil {
			return nil, err
		}

		switch msg.MessageType {
		case 'I':
			queryError = EmptyQuery
		default:
			c.handleStatelessMessage(msg)
		}
	}
	return nil, queryError
}

func (c *Connection) Close() error {
	defer c.resetConnection()
	return c.sendMessage(TerminateMessage())
}

func (connection *Connection) resetConnection() {
	connection.socket.Close()
	connection.parameters = make(map[string]string)
	connection.backendPid = 0
	connection.backendKey = 0
}

func (c *Connection) sendMessage(msg OutgoingMessage) error {
	return msg.Send(c.socket)
}

func (c *Connection) receiveMessage() (*IncomingMessage, error) {
	return ReadMessage(c.socket)
}
