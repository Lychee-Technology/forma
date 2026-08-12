package internal

import (
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/redact"
)

// undisclosedBatchError is the whole of what a failed batch operation reports
// when its error published nothing.
//
// It is the wording internal/httpapi writes in the same situation
// (publicErrorMessage, error_response.go), so the two write surfaces do not
// describe an undisclosed failure differently. That function has a second
// wording, "internal read error", which it reserves for the three federated
// read-path carriers errorClass recognises — ErrParquetSetInconsistent,
// ErrNoParquetPaths, ErrManifestSchemaMismatch. Not reproduced here, because
// none of them can arrive: all three are constructed in internal/federated and
// internal/manifest, and the batch executors are entityCRUDService.Create,
// Update and Delete, none of which reads through the federated engine — the
// CRUD service's one enrichment call sits in Get (entity_crud_service.go).
// Classifying here would add a branch that never fires and a second copy of a
// taxonomy that belongs to the HTTP layer.
//
// The caller is not left with only this string: forma.OperationError.Code
// carries the machine-readable classification of the failure alongside it
// (CREATE_FAILED, UPDATE_FAILED, DELETE_FAILED), and the call site logs the
// whole error, so nothing an operator needs is lost by publishing less.
const undisclosedBatchError = "internal error"

// resolveBatchErrorMessage renders one failed operation for
// forma.OperationError.Error.
//
// That field is exported and serialised into the response body (types.go), and
// executeBestEffortBatch is the only place in the tree that fills it: the atomic
// batch paths return the error itself, and every single-operation response goes
// through internal/httpapi, which puts the resolved published message in the
// body and never Error(). So this is the one write surface with no boundary in
// front of it, and it has to do the resolution itself.
//
// A publishing error answers its published message, scrubbed — the same two
// steps internal/httpapi applies to everything it writes (resolvePublicMessage
// then redactCredentials, respondErrorWithStatus). Resolution withholds two
// kinds of text Error() carries and a body must not: a forma.WithOperatorDetail
// attachment, which is how a failing write explains a stripped relation root
// (#318, explainStrippedRelationRoots), and the plain fmt.Errorf prefixes the
// write path adds above it, which name schema ids and internal steps. The scrub
// covers what resolution cannot — a published message is authored at a wrap
// site, and the text a wrap site interpolates is not always the author's: the
// registry publishes the caller's own schema name on a not-found
// (schemameta/file_registry.go), so a caller can put a DSN into a published
// batch message. The same matcher as the HTTP boundary and the CDC logger,
// because a second copy of it drifts (internal/redact).
//
// The same two steps, not the same string. internal/httpapi additionally
// prefixes the operation name; that prefix belongs to that layer.
//
// Anything else answers undisclosedBatchError, and deliberately nothing more.
// This used to render err.Error(), which published the write path's internal
// wraps and whatever the driver put inside them — a libpq connection string,
// password included, for a failed insert. docs/error-handling.md states the
// boundary without qualification: what crosses is PublicMessage(), never
// Error().
func resolveBatchErrorMessage(err error) string {
	published, ok := forma.ResolvePublicMessage(err)
	if !ok {
		return undisclosedBatchError
	}
	return redact.ConnStringPassword(published)
}
