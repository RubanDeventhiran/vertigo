package vertigo

import (
	"crypto/tls"
	"testing"
)

func defaultConnectionInfo() *ConnectionInfo {
	return &ConnectionInfo{Address: "127.0.0.1:5437", Username: "dbadmin"}
}

func TestConnecting(t *testing.T) {
	connection, err := Connect(defaultConnectionInfo())
	if err != nil {
		t.Fatal(err)
	}

	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSSLConnecting(t *testing.T) {
	info := defaultConnectionInfo()
	info.SslConfig = &tls.Config{InsecureSkipVerify: true}

	connection, err := Connect(info)
	if err != nil {
		t.Fatal(err)
	}

	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
}
