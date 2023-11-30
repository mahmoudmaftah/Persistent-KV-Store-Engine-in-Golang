package main

// Operation : <string>
type Operation string

// FileRecord : <Operation, string, string>
type FileRecord struct {
	Operation Operation
	Key       string
	Value     string
}

const (
	WalName string    = "mydb.wal"
	Put     Operation = "set"
	Del     Operation = "del"
)
