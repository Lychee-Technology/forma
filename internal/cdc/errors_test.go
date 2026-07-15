package cdc

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestResolveRequiredAttrCache(t *testing.T) {
	t.Run("registry lookup error", func(t *testing.T) {
		_, err := resolveRequiredAttrCache(errorSchemaRegistry{err: errors.New("boom")}, 7)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
		require.Contains(t, err.Error(), "7")
	})
	t.Run("empty cache", func(t *testing.T) {
		_, err := resolveRequiredAttrCache(stubSchemaRegistry{cache: forma.SchemaAttributeCache{}}, 9)
		require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
		require.Contains(t, err.Error(), "9")
	})
	t.Run("nil registry", func(t *testing.T) {
		_, err := resolveRequiredAttrCache(nil, 3)
		require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	})
	t.Run("populated cache", func(t *testing.T) {
		cache, err := resolveRequiredAttrCache(stubSchemaRegistry{cache: testAttrCache()}, 1)
		require.NoError(t, err)
		require.NotEmpty(t, cache)
	})
}
