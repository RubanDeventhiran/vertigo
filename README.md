# vertigo

Go client for Vertica anayltics database

### Simple usage

```go
package main

import (
  "github.com/wvanbergen/vertigo"
  "os"
  "fmt"
)

func main() {
  connection, err := vertigo.Connect("127.0.0.1:5437", "dbadmin")
  connection.Query("SELECT 1")
  connection.Close()
  os.Exit(0)
}

func checkError(err error) {
  if err != nil {
    fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
    os.Exit(1)
  }
}
```