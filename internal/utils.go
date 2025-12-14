package internal

import (
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func tryParseNumber(s string) any {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

func sanitizeIdentifier(name string) string {
	if name == "" {
		return ""
	}
	parts := strings.Split(name, ".")
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.Trim(part, " \"")
		if trimmed == "" {
			continue
		}
		clean = append(clean, trimmed)
	}
	if len(clean) == 0 {
		clean = []string{name}
	}
	return pgx.Identifier(clean).Sanitize()
}

func toUUID(obj any) (uuid.UUID, bool) {
	switch v := obj.(type) {
	case uuid.UUID:
		// 已经是 uuid.UUID 类型
		return v, true
	case *uuid.UUID:
		return *v, true
	case string:
		// 尝试解析字符串
		data, err := uuid.Parse(v)
		return data, err == nil
	case *string:
		if v == nil {
			return uuid.Nil, false
		}
		data, err := uuid.Parse(*v)
		return data, err == nil
	case []byte:
		// byte slice 可能是16字节的原始UUID或字符串形式
		if len(v) == 16 {
			// 16字节的原始UUID
			data, err := uuid.FromBytes(v)
			return data, err == nil
		}
		// 尝试作为字符串解析
		data, err := uuid.Parse(string(v))
		return data, err == nil
	default:
		return uuid.Nil, false
	}
}
