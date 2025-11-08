package internal

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestEncodeToBase32(t *testing.T) {
	data := []byte("hello world")
	expected := "nbswy5dpeb5w86tmmq"
	encoded := EncodeToBase32(data)
	assert.Equal(t, expected, encoded)
}

func TestDecodeFromBase32(t *testing.T) {
	s := "nbswy5dpeb5w86tmmq"
	expected := []byte("hello world")
	decoded, err := DecodeFromBase32(s)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)
}

func TestEncodeUUIDToBase32(t *testing.T) {
	id, _ := uuid.Parse("f81d4fae-7dec-11d0-a765-00a0c91e6bf6")
	expected := "9aou9lt77qi7bj5facqmshtl8y"
	encoded := EncodeUUIDToBase32(id)
	assert.Equal(t, expected, encoded)
}

func TestDecodeBase32ToUUID(t *testing.T) {
	s := "9aou9lt77qi7bj5facqmshtl8y"
	expected, _ := uuid.Parse("f81d4fae-7dec-11d0-a765-00a0c91e6bf6")
	decoded, err := DecodeBase32ToUUID(s)
	assert.NoError(t, err)
	assert.Equal(t, expected, decoded)
}

func TestBackAndForthUUID(t *testing.T) {
	originalID := uuid.Must(uuid.Parse("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"))
	encoded := EncodeUUIDToBase32(originalID)
	decodedID, err := DecodeBase32ToUUID(encoded)
	assert.NoError(t, err)
	assert.Equal(t, originalID, decodedID)
}

func TestBackAndForthString(t *testing.T) {
	original := []byte("hello world")
	encoded := EncodeToBase32(original)
	decoded, err := DecodeFromBase32(encoded)
	assert.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestDecodeFromBase32_Error(t *testing.T) {
	s := "invalid-base32-string"
	_, err := DecodeFromBase32(s)
	assert.Error(t, err)
}

func TestDecodeBase32ToUUID_Error(t *testing.T) {
	s := "invalid-base32-string"
	_, err := DecodeBase32ToUUID(s)
	assert.Error(t, err)

	// Test with a string that is a valid base32 but not a valid UUID
	s = "nbswy3dpeb3w64tmmq" // "hello world"
	_, err = DecodeBase32ToUUID(s)
	assert.Error(t, err)
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog.")
	encoded := EncodeToBase32(data)
	decoded, err := DecodeFromBase32(encoded)
	assert.NoError(t, err)
	assert.Equal(t, data, decoded)
}

func TestEncodeDecodeUUIDRoundtrip(t *testing.T) {
	id := uuid.New()
	encoded := EncodeUUIDToBase32(id)
	decoded, err := DecodeBase32ToUUID(encoded)
	assert.NoError(t, err)
	assert.Equal(t, id, decoded)
}
