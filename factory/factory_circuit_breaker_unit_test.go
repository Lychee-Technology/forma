package factory

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// Moved verbatim from factory_entity_manager_unit_test.go (#329): the DuckDB
// circuit-breaker constructor tests now live in their own file.

func TestNewDuckDBCircuitBreakerUsesDefaultParametersForZeroConfig(t *testing.T) {
	t.Parallel()

	breaker := newDuckDBCircuitBreaker(forma.DuckDBConfig{})

	for i := 0; i < 4; i++ {
		breaker.RecordFailure()
		assert.False(t, breaker.IsOpen())
	}
	breaker.RecordFailure()
	assert.True(t, breaker.IsOpen())
}

func TestNewDuckDBCircuitBreakerUsesConfiguredParameters(t *testing.T) {
	t.Parallel()

	breaker := newDuckDBCircuitBreaker(forma.DuckDBConfig{
		CircuitBreakerFailureThreshold: 2,
		CircuitBreakerWindow:           time.Minute,
		CircuitBreakerOpenDuration:     time.Minute,
	})

	breaker.RecordFailure()
	assert.False(t, breaker.IsOpen())
	breaker.RecordFailure()
	assert.True(t, breaker.IsOpen())
}

func TestNewDuckDBCircuitBreakerDoesNotWarnForDefaultThreshold(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	t.Cleanup(restore)

	breaker := newDuckDBCircuitBreaker(forma.DefaultConfig(nil).DuckDB)

	require.NotNil(t, breaker)
	assert.Equal(t, 0, logs.FilterMessage("circuitBreakerThreshold is deprecated and ignored; use circuitBreakerFailureThreshold instead").Len())
}

func TestNewDuckDBCircuitBreakerWarnsForDeprecatedThreshold(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	t.Cleanup(restore)

	breaker := newDuckDBCircuitBreaker(forma.DuckDBConfig{CircuitBreakerThreshold: 0.5})

	require.NotNil(t, breaker)
	assert.Equal(t, 1, logs.FilterMessage("circuitBreakerThreshold is deprecated and ignored; use circuitBreakerFailureThreshold instead").Len())
}
