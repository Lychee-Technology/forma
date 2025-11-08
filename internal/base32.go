package internal

import (
	"encoding/base32"

	"github.com/google/uuid"
)

const alphabet = "abcdefghijklmnopqrstuvwxyz156789"

var customEncoding = base32.NewEncoding(alphabet).WithPadding(base32.NoPadding)

func EncodeToBase32(data []byte) string {
	return customEncoding.EncodeToString(data)
}

func EncodeUUIDToBase32(id uuid.UUID) string {
	return EncodeToBase32(id[:])
}

func DecodeFromBase32(s string) ([]byte, error) {
	return customEncoding.DecodeString(s)
}

func DecodeBase32ToUUID(s string) (uuid.UUID, error) {
	data, err := DecodeFromBase32(s)
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.FromBytes(data)
}
