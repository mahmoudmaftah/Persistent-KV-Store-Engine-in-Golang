package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// WAL interface defines the methods for writing and reading records.
type WAL interface {
	WriteRecord(record FileRecord) error
	ReadRecord() (FileRecord, error)

	ResetWal() error
	SeekStart() error
	SeekEnd() error
	Close() error
}

// WALFile is a struct that implements the WAL interface and represents a WAL file.
type WALFile struct {
	hotVals     bool
	recordCount int
	file        *os.File
}

func NewWALFile(fileName string) (*WALFile, error) {
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &WALFile{file: file}, nil
}

func (w *WALFile) SeekStart() error {
	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		fmt.Println("Error seeking to the beginning of the WAL file:", err)
		return err
	}
	return nil
}

func (w *WALFile) SeekEnd() error {
	_, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		fmt.Println("Error seeking to the end of the WAL file:", err)
		return err
	}
	return nil
}

func (w *WALFile) ResetWal() error {
	err := w.file.Truncate(0)
	if err != nil {
		return err
	}

	_, err = w.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	return nil
}

func (w *WALFile) WriteRecord(record FileRecord) error {

	// First seek the end of the File.
	if err := w.SeekEnd(); err != nil {
		return err
	}

	// Convert record to JSON
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// Write the length of the record
	if err := binary.Write(w.file, binary.BigEndian, int64(len(data))); err != nil {
		return err
	}

	// Write the record data
	_, err = w.file.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (w *WALFile) ReadRecord() (FileRecord, error) {
	// Read the length of the record
	var length int64
	if err := binary.Read(w.file, binary.BigEndian, &length); err != nil {
		if err == io.EOF {
			return FileRecord{}, io.EOF
		}
		return FileRecord{}, err
	}

	// Read the record data
	data := make([]byte, length)
	_, err := io.ReadFull(w.file, data)
	if err != nil {
		return FileRecord{}, err
	}

	// Unmarshal the JSON data into a WALRecord
	var record FileRecord
	err = json.Unmarshal(data, &record)
	if err != nil {
		return FileRecord{}, err
	}

	return record, nil
}

func (w *WALFile) Close() error {
	return w.file.Close()
}
