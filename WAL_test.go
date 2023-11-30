package main

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWALFile(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "wal_test")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Create a new WALFile instance
	wal, err := NewWALFile(tmpFile.Name())
	assert.NoError(t, err)
	defer wal.Close()

	// Test WriteRecord
	record := FileRecord{
		Operation: "set",
		Key:       "testKey",
		Value:     "testValue",
	}
	err = wal.WriteRecord(record)
	assert.NoError(t, err)

	// Test SeekStart and ReadRecord
	err = wal.SeekStart()
	assert.NoError(t, err)

	readRecord, err := wal.ReadRecord()
	assert.NoError(t, err)
	assert.Equal(t, record, readRecord)

	// Test ResetWal
	err = wal.ResetWal()
	assert.NoError(t, err)

	// Test SeekEnd after ResetWal
	err = wal.SeekEnd()
	assert.NoError(t, err)

	// Test Close
	err = wal.Close()
	assert.NoError(t, err)

	// Test reopening the file and reading after closing
	reopenedWal, err := NewWALFile(tmpFile.Name())
	assert.NoError(t, err)
	defer reopenedWal.Close()

	// Ensure the file is empty after reset and closing
	reopenedWal.SeekStart()
	emptyRecord, err := reopenedWal.ReadRecord()
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, FileRecord{}, emptyRecord)
}
