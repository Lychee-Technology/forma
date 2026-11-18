package manifestv2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
)

// ErrKeyExists reports a write-once put to a key that already holds an
// object. The object is left unchanged, and the put is never retried as an
// overwrite. Keys are fresh by construction (a version 7 UUID, a generation
// token), so a writer that meets it abandons the staging and stages under new
// keys. That includes the case where the store's client retried a put whose
// first answer was lost and the first attempt had landed: the collision then
// is with the writer's own object, and abandoning it is still safe, since
// garbage collection reclaims an object no commit names.
var ErrKeyExists = errors.New("object key already written")

// ErrObjectNotFound reports a get of a key that holds no object.
var ErrObjectNotFound = errors.New("object not found")

// Store is a write-once object store: every object is written once and only
// garbage collection deletes it. S3Store and FSStore share the contract, so
// unit tests exercise the one the deployment uses.
type Store interface {
	// Put writes body under key if, and only if, no object exists there,
	// and answers ErrKeyExists otherwise. A reader never sees a partly
	// written object.
	Put(ctx context.Context, key string, body io.ReadSeeker) error
	// Get answers the object's bytes, or ErrObjectNotFound.
	Get(ctx context.Context, key string) ([]byte, error)
}

// PutManifest encodes m and writes it once to its key under l, and answers
// the key and the digest that Publish then commits.
func PutManifest(ctx context.Context, st Store, l Layout, m Manifest) (key, digest string, err error) {
	raw, digest, err := m.Encode()
	if err != nil {
		return "", "", err
	}
	key = l.ManifestKey(m.SchemaID, m.Epoch, m.Generation, m.Token)
	if err := st.Put(ctx, key, bytes.NewReader(raw)); err != nil {
		return "", "", fmt.Errorf("stage manifest of schema %d generation %d: %w", m.SchemaID, m.Generation, err)
	}
	return key, digest, nil
}
