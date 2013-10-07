package vertigo

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

var (
	SslNotSupported                  = errors.New("SSL not available on this server")
	AuthenticationMethodNotSupported = errors.New("Authentication method not supported")
)

type ConnectionInfo struct {
	Address   string
	User      string
	Database  string
	Password  string
	SslConfig *tls.Config
}

type Connection struct {
	l sync.Mutex

	socket            net.Conn
	parameters        map[string]string
	backendPid        uint32
	backendKey        uint32
	transactionStatus byte
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
		connection.sendMessage(SSLRequestMessage{})

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

func (c *Connection) handleStatelessMessage(msg IncomingMessage) {
	switch msg := msg.(type) {
	case ParameterStatusMessage:
		c.parameters[msg.Name] = msg.Value

	case BackendKeyDataMessage:
		c.backendPid = msg.Pid
		c.backendKey = msg.Key

	case ReadyForQueryMessage:
		c.transactionStatus = msg.TransactionStatus

	default:
		panic(fmt.Sprintf("Unexpected message: %#+v", msg))
	}
}

func (c *Connection) initConnection(info *ConnectionInfo) error {

	c.sendMessage(StartupMessage{User: info.User, Database: info.Database})
	for {
		msg, err := c.receiveMessage()
		if err != nil {
			return err
		}

		switch msg := msg.(type) {
		case AuthenticationRequestMessage:
			switch msg.AuthCode {
			case AuthenticationOK:
				continue
			case AuthenticationCleartextPassword:
				c.sendMessage(PasswordMessage{Password: info.Password, AuthenticationMethod: msg.AuthCode})
			default:
				return AuthenticationMethodNotSupported
			}

		case ErrorResponseMessage:
			return msg

		default:
			c.handleStatelessMessage(msg)
		}

		if _, ok := msg.(ReadyForQueryMessage); ok {
			return nil
		}

	}
	return nil
}

func (c *Connection) Query(sql string) (resultset *Resultset, queryError error) {
	c.l.Lock()
	defer c.l.Unlock()

	if queryError = c.sendMessage(QueryMessage{SQL: sql}); queryError != nil {
		return
	}

	for {
		if msg, err := c.receiveMessage(); err != nil {
			queryError = err
			return

		} else {

			switch msg := msg.(type) {
			case EmptyQueryMessage:
				queryError = msg

			case ErrorResponseMessage:
				queryError = msg

			case RowDescriptionMessage:
				resultset = &Resultset{Fields: msg.Fields}

			case DataRowMessage:
				resultset.Rows = append(resultset.Rows, Row{Values: msg.Values})

			case CommandCompleteMessage:
				resultset.Result = msg.Result

			default:
				c.handleStatelessMessage(msg)
			}

			if _, ok := msg.(ReadyForQueryMessage); ok {
				break
			}
		}
	}
	return
}

func (c *Connection) Close() error {
	defer c.resetConnection()
	return c.sendMessage(TerminateMessage{})
}

func (connection *Connection) resetConnection() {
	connection.socket.Close()
	connection.parameters = make(map[string]string)
	connection.backendPid = 0
	connection.backendKey = 0
}

func (c *Connection) sendMessage(msg OutgoingMessage) error {
	if TrafficLogger != nil {
		TrafficLogger.Printf("=> %#+v\n", msg)
	}

	return SendMessage(c.socket, msg)
}

func (c *Connection) receiveMessage() (msg IncomingMessage, err error) {
	msg, err = ReadMessage(c.socket)
	if err == nil && TrafficLogger != nil {
		TrafficLogger.Printf("<= %#+v", msg)
	}
	return
}
