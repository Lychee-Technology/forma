package manifestv2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// FSStore is a write-once Store over a local directory, with S3Store's
// semantics, so unit tests exercise the contract the deployment relies on.
// A key is any non-empty string, as on S3: FSStore maps it to a path under Dir
// one slash-separated segment at a time (fsName), so the key of an empty
// Layout prefix ("/100/...") is held under its own name, distinct keys
// ("/x" and "x", "a//b" and "a/b") never share a file, and no key reaches
// outside Dir. Unlike S3, a key cannot be both an object and the directory of
// another ("a" and "a/b"); no Layout builds such a pair.
type FSStore struct {
	Dir string
}

// Put writes body to a temporary file beside the key and hard-links it to the
// key. The link fails when the key exists, which makes the write once-only
// for every racing writer, and a reader of the key sees the whole object or
// none of it.
func (f *FSStore) Put(ctx context.Context, key string, body io.ReadSeeker) error {
	root, rel, err := f.open(key)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("put object %s: %w", key, err)
	}
	dir := filepath.Dir(rel)
	if err := root.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create the directory of object %s: %w", key, err)
	}
	tmp := filepath.Join(dir, "."+filepath.Base(rel)+"."+uuid.NewString()+".tmp")
	err = writeFile(root, tmp, body)
	defer func() { _ = root.Remove(tmp) }()
	if err != nil {
		return fmt.Errorf("stage object %s: %w", key, err)
	}
	err = root.Link(tmp, rel)
	if errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("put object %s: %w", key, ErrKeyExists)
	}
	if err != nil {
		return fmt.Errorf("put object %s: %w", key, err)
	}
	return nil
}

// Get answers the object's bytes, or ErrObjectNotFound.
func (f *FSStore) Get(ctx context.Context, key string) ([]byte, error) {
	root, rel, err := f.open(key)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	data, err := root.ReadFile(rel)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("get object %s: %w: %w", key, ErrObjectNotFound, err)
	}
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	return data, nil
}

func (f *FSStore) open(key string) (*os.Root, string, error) {
	if f == nil || f.Dir == "" {
		return nil, "", errors.New("fs store is not configured")
	}
	if key == "" {
		return nil, "", errors.New("object key is empty")
	}
	segments := strings.Split(key, "/")
	for i, s := range segments {
		segments[i] = fsName(s)
	}
	rel := path.Join(segments...)
	if !fs.ValidPath(rel) {
		return nil, "", fmt.Errorf("object key %q maps to %q, not a path the fs store can hold", key, rel)
	}
	root, err := os.OpenRoot(f.Dir)
	if err != nil {
		return nil, "", fmt.Errorf("open fs store %s: %w", f.Dir, err)
	}
	return root, filepath.FromSlash(rel), nil
}

// fsName is the file name of one key segment. It is injective, so two keys
// never map to one path: "%" becomes "%25" and a leading "." becomes "%2E"
// (so a "%" in a name is always followed by "25" or "2E"), and the empty
// segment, of a leading or a doubled slash, becomes a lone "%". No name it
// answers is empty, "." or "..", or starts with ".", the mark of Put's
// temporary files.
func fsName(segment string) string {
	if segment == "" {
		return "%"
	}
	name := strings.ReplaceAll(segment, "%", "%25")
	if rest, ok := strings.CutPrefix(name, "."); ok {
		name = "%2E" + rest
	}
	return name
}

func writeFile(root *os.Root, name string, body io.Reader) error {
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	if _, err := io.Copy(file, body); err != nil {
		_ = file.Close()
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync %s: %w", name, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", name, err)
	}
	return nil
}
