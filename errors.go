package forma

import "errors"

// Sentinel errors returned by the entity manager layer.
// Callers should use errors.Is / errors.As for matching; never compare strings.

// ErrNotFound is returned when a requested entity does not exist.
var ErrNotFound = errors.New("not found")

// ErrConflict is returned when an operation would violate a uniqueness constraint
// or produce a duplicate record.
var ErrConflict = errors.New("conflict")

// ErrInvalidInput is returned when caller-supplied input fails validation
// (missing required fields, unsupported values, etc.).
var ErrInvalidInput = errors.New("invalid input")
