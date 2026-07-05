package transform

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestToUUID(t *testing.T) {
	u := uuid.New()
	uPtr := uuid.MustParse(u.String())
	validStr := u.String()
	validStrPtr := &validStr
	raw16 := u[:]
	strBytes := []byte(validStr)
	invalidStr := "not-a-uuid"

	tests := []struct {
		name   string
		input  any
		expect uuid.UUID
		ok     bool
	}{
		{name: "uuid value", input: u, expect: u, ok: true},
		{name: "uuid pointer", input: &uPtr, expect: uPtr, ok: true},
		{name: "uuid pointer nil", input: (*uuid.UUID)(nil), expect: uuid.Nil, ok: false},
		{name: "string valid", input: validStr, expect: u, ok: true},
		{name: "string pointer valid", input: validStrPtr, expect: u, ok: true},
		{name: "string invalid", input: invalidStr, expect: uuid.Nil, ok: false},
		{name: "string pointer nil", input: (*string)(nil), expect: uuid.Nil, ok: false},
		{name: "bytes raw16", input: raw16, expect: u, ok: true},
		{name: "bytes string form", input: strBytes, expect: u, ok: true},
		{name: "bytes invalid", input: []byte("bad-bytes"), expect: uuid.Nil, ok: false},
		{name: "unsupported type", input: 123, expect: uuid.Nil, ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toUUID(tt.input)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.expect, got)
		})
	}
}
