package vertigo

import (
	"log"
	"os"
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

	if _, err := connection.Query(""); err != EmptyQuery {
		t.Fatal("Expected empty query to return an empty query error.")
	}

	if _, err := connection.Query(" -- empty "); err != EmptyQuery {
		t.Fatal("Expected comment query to return an empty query error.")
	}
}

func TestQueryWithoutResults(t *testing.T) {
	connection := getConnection(t)
	defer connection.Close()

	var (
		resultset *Resultset
		err       error
	)

	if resultset, err = connection.Query("SELECT 'foo' AS test LIMIT 0"); err != nil {
		t.Fatal(err)
	}

	if resultset == nil {
		t.Fatal("Expected a resultset object")
	}

	if len(resultset.Fields) != 1 {
		t.Fatalf("Was expecting only one field, but found %d", len(resultset.Fields))
	}

	if resultset.Fields[0].Name != "test" {
		t.Fatalf("Was expecting first field to be called 'test', but was %s", resultset.Fields[0].Name)
	}

	if len(resultset.Rows) != 0 {
		t.Fatalf("Was expecting zero rows, but found %d", len(resultset.Fields))
	}
}

func TestQueryWithResults(t *testing.T) {
	TrafficLogger = log.New(os.Stdout, "", log.LstdFlags)
	defer func() { TrafficLogger = nil }()

	connection := getConnection(t)
	defer connection.Close()

	var (
		resultset *Resultset
		err       error
	)

	sql := `
		SELECT TRUE, 1, 1.1, '1.1'::numeric, 'test', NOW()::DATE, NOW()
		UNION ALL
		SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL
	`
	if resultset, err = connection.Query(sql); err != nil {
		t.Fatal(err)
	}

	if len(resultset.Fields) != 7 {
		t.Fatalf("Was expecting seven fields, but found %d", len(resultset.Fields))
	}
}
