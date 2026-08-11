package internal

import "github.com/lychee-technology/forma"

// batchErrorMessage renders one failed operation for forma.OperationError.Error.
//
// That field is exported and serialised into the response body (types.go), and
// executeBestEffortBatch is the only place in the tree that fills it: the atomic
// batch paths return the error itself, and every single-operation response goes
// through internal/httpapi, which puts the resolved published message in the
// body and never Error(). So this is the one write surface with no boundary in
// front of it, and it has to do the resolution itself.
//
// A publishing error answers its published message: the same resolution the
// HTTP boundary emits, which withholds two kinds of text Error() carries and a
// body must not — a forma.WithOperatorDetail attachment, which is how a failing
// write explains a stripped relation root (#318,
// explainStrippedRelationRoots), and the plain fmt.Errorf prefixes the write
// path adds above it, which name schema ids and internal steps.
//
// The same resolution, not the same string. internal/httpapi additionally
// prefixes the operation name and scrubs connection-string credentials from what
// it writes (respondErrorWithStatus, redact.ConnStringPassword); neither is
// reproduced here. The prefix belongs to that layer, and the scrub is defence in
// depth over a message authored at a wrap site — applying it here is part of the
// wider question of what a batch result may carry, which is filed separately.
//
// Anything else falls back to Error(), which is what every batch failure
// rendered before. That fallback may still expose internal context; narrowing it
// would change every batch error response, is not what #318 introduced, and is
// filed separately too.
//
// The fallback cannot carry an operator-detail attachment, and that is decided
// by the constructor rather than asserted here: forma.WithOperatorDetail returns
// its input unchanged unless forma.ResolvePublicMessage accepts it
// (client_error.go), so an operator-detail node is only ever built over a
// publishing error — and the wraps this package puts above one, fmt.Errorf's %w
// and forma.WrapPublicf, are all traversed by that same resolver.
// TestWriteDiagnosisIsSilentOnOperatorClassErrors pins the near miss: an
// operator-class validation failure picks up no diagnosis at all.
//
// The log line at the call site keeps the whole error, so nothing an operator
// needs is lost by publishing less here.
func batchErrorMessage(err error) string {
	if published, ok := forma.ResolvePublicMessage(err); ok {
		return published
	}
	return err.Error()
}
