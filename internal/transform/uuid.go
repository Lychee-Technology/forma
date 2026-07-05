package transform

import "github.com/google/uuid"

func toUUID(obj any) (uuid.UUID, bool) {
	switch v := obj.(type) {
	case uuid.UUID:
		return v, true
	case *uuid.UUID:
		if v == nil {
			return uuid.Nil, false
		}
		return *v, true
	case string:
		data, err := uuid.Parse(v)
		return data, err == nil
	case *string:
		if v == nil {
			return uuid.Nil, false
		}
		data, err := uuid.Parse(*v)
		return data, err == nil
	case []byte:
		if len(v) == 16 {
			data, err := uuid.FromBytes(v)
			return data, err == nil
		}
		data, err := uuid.Parse(string(v))
		return data, err == nil
	default:
		return uuid.Nil, false
	}
}
