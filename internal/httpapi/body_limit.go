package httpapi

import (
	"errors"
	"net/http"

	"github.com/lychee-technology/forma"
)

// defaultMaxBodyBytes is the request-body cap a Server applies when Options
// names none: forma's declared entity size limit (#465). The cap is applied
// before the decoder reads a byte past it, so an oversized body is refused
// without ever being materialized.
var defaultMaxBodyBytes = int64(forma.DefaultConfig(nil).Entity.MaxEntitySize)

// bodyLimit returns the number of bytes a request body may carry.
func (s *Server) bodyLimit() int64 {
	if s.opts.MaxBodyBytes > 0 {
		return s.opts.MaxBodyBytes
	}
	return defaultMaxBodyBytes
}

// respondBodyError answers a failed body decode. A body that overran the cap
// is a 413 whose published message names the limit the caller has to fit
// under; the message is authored here from the limit alone, never from
// request text. Every other decode failure keeps the #360 shape: a disclosed
// 400 built from encoding/json's own prose.
func respondBodyError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		respondErrorWithStatus(w, http.StatusRequestEntityTooLarge, "request body too large",
			forma.InvalidInputf("request body exceeds %d bytes", tooLarge.Limit))
		return
	}
	respondError(w, "invalid json body", forma.InvalidInputf("%v", err))
}
