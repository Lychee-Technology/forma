package schemameta

import (
	"errors"
	"fmt"
	"sort"
)

// schemaFailures collects the per-schema load failures of one registry
// construction, so a directory or registry table with several bad schemas is
// diagnosed in one boot attempt instead of one schema per restart (#604). The
// registry stays fail-closed: any recorded failure still refuses construction.
type schemaFailures struct {
	failures []schemaFailure
}

type schemaFailure struct {
	schema string
	err    error
}

// add records err as the diagnosis of schema. err must already name the
// schema: err() keeps each diagnosis as its own line, unprefixed.
func (f *schemaFailures) add(schema string, err error) {
	f.failures = append(f.failures, schemaFailure{schema: schema, err: err})
}

// err answers nil when nothing failed, the lone diagnosis unchanged when one
// schema failed, and otherwise every diagnosis sorted by schema name, one per
// line, under a count, so the boot log is stable whatever order the schemas
// were loaded in. errors.Is and errors.As reach each diagnosis.
func (f *schemaFailures) err() error {
	switch len(f.failures) {
	case 0:
		return nil
	case 1:
		return f.failures[0].err
	}
	sorted := make([]schemaFailure, len(f.failures))
	copy(sorted, f.failures)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].schema < sorted[j].schema })
	errs := make([]error, len(sorted))
	for i, failure := range sorted {
		errs[i] = failure.err
	}
	return fmt.Errorf("%d schemas failed to load:\n%w", len(errs), errors.Join(errs...))
}
