package vertigo

import (
	// "log"
	// "os"
	"testing"
)

func getConnection(t *testing.T) Connection {
	connection, connectionErr := Connect(defaultConnectionInfo())
	if connectionErr != nil {
		t.Fatal(connectionErr)
	}
	return connection
}

func TestNonQuery(t *testing.T) {
	connection := getConnection(t)
	defer connection.Close()

	if _, err := connection.Query(" -- empty "); err != EmptyQuery {
		t.Fatal("Expected empty query to return an empty query error.")
	}

	if _, err := connection.Query(" -- empty "); err != EmptyQuery {
		t.Fatal("Expected empty query to return an empty query error.")
	}
}

// func TestQueryWithoutResults(t *testing.T) {
// 	TrafficLogger = log.New(os.Stdout, "", log.LstdFlags)
// 	defer func() { TrafficLogger = nil }()

// 	connection := getConnection(t)
// 	defer connection.Close()

// 	var (
// 		resultset [][]interface{}
// 		err       error
// 	)

// 	if resultset, err = connection.Query("SELECT 'foo' LIMIT 0"); err != nil {
// 		t.Fatal(err)
// 	}

// 	if resultset == nil {
// 		t.Fatal("Expected a resultset object")
// 	}
// }
