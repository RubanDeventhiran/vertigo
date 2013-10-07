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

func isEmptyQuery(msg IncomingMessage) bool {
	_, ok := msg.(EmptyQueryMessage)
	return ok
}

func TestNonQuery(t *testing.T) {
	connection := getConnection(t)
	defer connection.Close()

	if _, err := connection.Query(""); !isEmptyQuery(err) {
		t.Fatal("Expected empty query to return an empty query error.")
	}

	if _, err := connection.Query(" -- empty "); !isEmptyQuery(err) {
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
		SELECT NULL::boolean, NULL::int, NULL::float, NULL::numeric, NULL::varchar, NULL::date, NULL::datetime
	`
	if resultset, err = connection.Query(sql); err != nil {
		t.Fatal(err)
	}

	if len(resultset.Fields) != 7 {
		t.Fatalf("Was expecting seven fields, but found %d", len(resultset.Fields))
	}

	if len(resultset.Rows) != 2 {
		t.Fatalf("Was expecting two rows, but found %d", len(resultset.Rows))
	}

	// TODO: test value of the first row.

	for i, value := range resultset.Rows[1].Values {
		if value != nil {
			t.Fatalf("Expected NULL for column %d but found %#+v", i, resultset.Rows[1].Values[0])
		}
	}
}

func TestQueryWithSyntaxError(t *testing.T) {
	connection := getConnection(t)
	defer connection.Close()

	if _, err := connection.Query("SELECT /ERROR"); err == nil {
		t.Fatal("Expected an error response")
	}
}
