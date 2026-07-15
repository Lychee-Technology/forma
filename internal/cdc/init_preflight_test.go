package cdc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestProcessInitSchemas_AbortsWhenSchemaCacheUnavailable(t *testing.T) {
	runCtx := &initRunContext{
		logger:         zap.NewNop(),
		schemaRegistry: errorSchemaRegistry{err: errors.New("schema unavailable")},
	}

	summary, err := processInitSchemas(context.Background(), runCtx, []int64{7})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	require.Contains(t, err.Error(), "7")
	require.Equal(t, InitSummary{}, summary)
}
