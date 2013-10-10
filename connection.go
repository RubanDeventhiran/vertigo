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

// Struct to hold all the information necessary to connect to the Vertics server.
type ConnectionInfo struct {
	Address   string      // The address of the Vertica server. Should include a port number.
	User      string      // The user to connect with.
	Password  string      // The password for this user.
	Database  string      // The database to connect to. This can be left empty.
	SslConfig *tls.Config // The tls.Config struct to use for SSL connections.
}

// The main connection object.
type Connection struct {
	l sync.Mutex // Connection lock to make sure only one command runs at a time

	config            *ConnectionInfo   // Holds the connection parameters
	socket            net.Conn          // The network socket of this connection
	parameters        map[string]string // Server parameters the client gets told about when connecting
	backendPid        uint32            // The PID of the server's process.
	backendKey        uint32            // The secret key of the server's backend process.
	transactionStatus byte              // The current transaction status of the connection
}

// Opens a connection to the server using the information in the config parameter.
//
// The connection will be returned as the first return value. It will only be in a
// usuable state if the second return value is nil.
func Connect(config *ConnectionInfo) (connection Connection, connectionError error) {
	connection = Connection{config: config}
	defer func() {
		if r := recover(); r != nil {
			connection.resetConnection()
			connectionError = r.(error)
		}
	}()

	connection.l.Lock()
	defer connection.l.Unlock()

	connection.resetConnection()
	connection.openConnection()
	return connection, nil
}

// Returns the current transaction status of the connection.
func (c *Connection) TransactionStatus() byte {
	return c.transactionStatus
}

// Closes the connection to the server.
//
// It will try to gracefully terminate the connection by sending the server
// a terminate message. Regardless of whether this succeeds, the socket will be
// closed and the status of the connection will be reset.
func (c *Connection) Close() (err error) {
	defer c.resetConnection()
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	if c.socket != nil {
		c.sendMessage(TerminateMessage{})
	} else {
		panic("Socket is not open")
	}

	return nil
}

// Runs a SQL connection on the server.
//
// If the query succeeds, the resultset will be returned as the first return value.
// When the server returns an error response, this will be returned as the second
// return value.
//
// If a connection occurs, the state of the connection is undeterministic, so
// the connection will be closed, and the connection error will be returned as
// the second return value. The connection will automatically try to reconnect
// if you try to use it for a query again.
func (c *Connection) Query(sql string) (resultset *Resultset, queryError error) {
	c.l.Lock()
	defer c.l.Unlock()

	defer func() {
		if r := recover(); r != nil {
			c.resetConnection()
			queryError = r.(error)
		}
	}()

	if c.socket == nil {
		c.openConnection()
	}

	c.sendMessage(QueryMessage{SQL: sql})
	for msg := c.receiveMessage(); !c.isReadyForQuery(msg); msg = c.receiveMessage() {
		switch msg := msg.(type) {
		case EmptyQueryMessage, ErrorResponseMessage:
			resultset = nil
			queryError = msg.(error)

		case RowDescriptionMessage:
			resultset = &Resultset{Fields: msg.Fields}

		case DataRowMessage:
			resultset.Rows = append(resultset.Rows, Row{Values: msg.Values})

		case CommandCompleteMessage:
			resultset.Result = msg.Result

		default:
			c.handleStatelessMessage(msg)
		}
	}
	return
}

// Handles any message from the server that falls outside the stateful parts of
// the protocol.
func (c *Connection) handleStatelessMessage(msg IncomingMessage) {
	switch msg := msg.(type) {
	case ParameterStatusMessage:
		c.parameters[msg.Name] = msg.Value

	case BackendKeyDataMessage:
		c.backendPid = msg.Pid
		c.backendKey = msg.Key

	default:
		panic(fmt.Errorf("Unexpected message: %#+v", msg))
	}
}

// Opens the TCP socket, and optionally initializes the TLS encryption on it.
// This function will panic if something goes wrong when connecting.
func (c *Connection) openConnection() {
	if socket, dialError := net.Dial("tcp", c.config.Address); dialError != nil {
		panic(dialError)
	} else {
		c.socket = socket
	}

	if c.config.SslConfig != nil {
		c.sendMessage(SSLRequestMessage{})

		sslResponse := make([]byte, 1)
		io.ReadFull(c.socket, sslResponse)
		if sslResponse[0] == byte('S') {
			tlsSocket := tls.Client(c.socket, c.config.SslConfig)
			c.socket = tlsSocket
			if tlsError := tlsSocket.Handshake(); tlsError != nil {
				panic(tlsError)
			}
		} else {
			panic(SslNotSupported)
		}
	}

	c.authenticateConnection()
}

// Initializes the connection by doing the initial authenentication message
// This function will panic when skmething goes wrong while connecting.
func (c *Connection) authenticateConnection() {
	c.sendMessage(StartupMessage{User: c.config.User, Database: c.config.Database})

	for msg := c.receiveMessage(); !c.isReadyForQuery(msg); msg = c.receiveMessage() {
		switch msg := msg.(type) {
		case AuthenticationRequestMessage:
			switch msg.AuthCode {
			case AuthenticationOK:
				continue
			case AuthenticationCleartextPassword:
				c.sendMessage(PasswordMessage{Password: c.config.Password, AuthenticationMethod: msg.AuthCode})
			default:
				panic(AuthenticationMethodNotSupported)
			}

		case ErrorResponseMessage:
			panic(msg)

		default:
			c.handleStatelessMessage(msg)
		}
	}
	return
}

// Checks whether the message from the server is a ReadyForQuery (Z)
// message. If so, the transaction status for the connection is set.
func (c *Connection) isReadyForQuery(msg IncomingMessage) bool {
	typeMsg, ok := msg.(ReadyForQueryMessage)
	if ok {
		c.transactionStatus = typeMsg.TransactionStatus
	}
	return ok
}

// Resets the connection. This will try to close the socket connection
// if it still exists, and will reset all the connection status variables
// to their default vaules.
func (c *Connection) resetConnection() {
	if c.socket != nil {
		c.socket.Close()
		c.socket = nil
	}

	c.parameters = make(map[string]string)
	c.backendPid = 0
	c.backendKey = 0
	c.transactionStatus = 0
}

// Send a message to the server.
//
// This method will log the message to the TrafficLogger if the
// Traffic logger is set to a logger instance.
func (c *Connection) sendMessage(msg OutgoingMessage) {
	err := sendMessage(c.socket, msg)
	if err != nil {
		panic(err)
	}

	if TrafficLogger != nil {
		TrafficLogger.Printf("=> %#+v\n", msg)
	}
}

// Receive a message from the server.
//
// This method will log the message to the TrafficLogger if the
// Traffic logger is set to a logger instance.
func (c *Connection) receiveMessage() IncomingMessage {
	msg, err := receiveMessage(c.socket)
	if err != nil {
		panic(err)
	}

	if TrafficLogger != nil {
		TrafficLogger.Printf("<= %#+v", msg)
	}

	return msg
}
