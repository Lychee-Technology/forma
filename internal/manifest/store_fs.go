package manifest

import (
	"context"
	"fmt"
	"io/fs"
	"os"
)

// FSStore implements Store on local filesystem.
type FSStore struct {
	Root fs.FS
}

func (f *FSStore) Load(ctx context.Context, path string) ([]byte, string, error) {
	if f == nil || f.Root == nil {
		return nil, "", fmt.Errorf("fs store not configured")
	}
	b, err := fs.ReadFile(f.Root, path)
	if err != nil {
		return nil, "", err
	}
	return b, "", nil
}

func (f *FSStore) Save(ctx context.Context, path string, data []byte, etag string) (string, error) {
	// NOTE: ignores etag; for local dev/testing only.
	if f == nil || f.Root == nil {
		return "", fmt.Errorf("fs store not configured")
	}
	// Root must be os.DirFS-compatible to allow WriteFile via os
	dir, ok := f.Root.(fs.StatFS)
	if !ok {
		return "", fmt.Errorf("fs store root not writable")
	}
	_ = dir // keep linter calm; actual write below uses os package with path
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", err
	}
	return "", nil
}
