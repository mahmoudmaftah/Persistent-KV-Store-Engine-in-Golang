package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPersMem(t *testing.T) {
	// Create a new PersMem instance
	cache, err := NewPersMem()
	assert.NoError(t, err)
	defer cache.Close()

	// Test SetM and GetM
	err = cache.SetM("testKey", "testValue")
	assert.NoError(t, err)

	value, err := cache.GetM("testKey")
	assert.NoError(t, err)
	assert.Equal(t, Tuple{"set", "testValue"}, value)

	// Test DelM
	deletedValue, err := cache.DelM("testKey")
	assert.NoError(t, err)
	assert.Equal(t, "testValue", deletedValue)

	// Test GetM for a key that is completely new to the cache.
	_, err = cache.GetM("NewKey")
	assert.Error(t, err, "Key Not Found In MemDB")

	// Test GetM after deletion
	tupl, err := cache.GetM("testKey")
	assert.NoError(t, err)
	assert.Equal(t, Tuple{"del", ""}, tupl)

	// Test Clear
	err = cache.Clear()
	assert.NoError(t, err)

	// Test GetM after clearing
	_, err = cache.GetM("testKey")
	assert.Error(t, err, "Key Not found In MemDB")
}
