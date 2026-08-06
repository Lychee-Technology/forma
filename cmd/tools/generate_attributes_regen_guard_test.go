package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// shippedSchemasDir points at the schema artifacts this repo ships. Both
// guards below fail loudly if discovery finds nothing, so a moved directory
// cannot silently disable them.
const shippedSchemasDir = "../server/schemas"

// firstDivergence returns a printable window around the first byte where a
// and b differ, so a guard failure names the drift instead of just "bytes
// differ".
func firstDivergence(a, b []byte) string {
	limit := len(a)
	if len(b) < limit {
		limit = len(b)
	}
	i := 0
	for i < limit && a[i] == b[i] {
		i++
	}
	start := i - 80
	if start < 0 {
		start = 0
	}
	end := i + 80
	clamp := func(s []byte) string {
		e := end
		if e > len(s) {
			e = len(s)
		}
		if start >= len(s) {
			return ""
		}
		return string(s[start:e])
	}
	return "committed: ..." + clamp(a) + "...\nregenerated: ..." + clamp(b) + "..."
}

// TestShippedAttributesRegenerateByteIdentical seeds each committed
// *_attributes.json onto a scratch path (the generator preserves
// attributeIDs only when the output file already exists — #315), regenerates
// it from its schema, and requires byte identity. A failure means an edit to
// a shipped schema (or to the generator) would retype or shift physical EAV
// attributeIDs; regenerate deliberately and review the diff instead of
// weakening this test.
func TestShippedAttributesRegenerateByteIdentical(t *testing.T) {
	matches, err := filepath.Glob(filepath.Join(shippedSchemasDir, "*_attributes.json"))
	if err != nil {
		t.Fatalf("glob shipped attributes files: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("no *_attributes.json found under %s — guard is scanning the wrong directory", shippedSchemasDir)
	}
	for _, committedPath := range matches {
		name := strings.TrimSuffix(filepath.Base(committedPath), "_attributes.json")
		t.Run(name, func(t *testing.T) {
			schemaPath := filepath.Join(shippedSchemasDir, name+".json")
			committed, err := os.ReadFile(committedPath)
			if err != nil {
				t.Fatalf("read committed attributes: %v", err)
			}
			scratch := filepath.Join(t.TempDir(), name+"_attributes.json")
			if err := os.WriteFile(scratch, committed, 0o644); err != nil {
				t.Fatalf("seed scratch attributes: %v", err)
			}
			if err := generateAttributesJSON(schemaPath, scratch, false); err != nil {
				t.Fatalf("regenerate attributes for %s: %v", name, err)
			}
			regenerated, err := os.ReadFile(scratch)
			if err != nil {
				t.Fatalf("read regenerated attributes: %v", err)
			}
			if !bytes.Equal(committed, regenerated) {
				t.Errorf("%s_attributes.json does not regenerate byte-identically (committed %d bytes, regenerated %d bytes)\n%s",
					name, len(committed), len(regenerated), firstDivergence(committed, regenerated))
			}
		})
	}
}

// TestShippedFullSchemasMatchInlineOutput pins every committed *_full.json
// to the inline-schema tool's output for its base schema, closing the drift
// channel where schema edits land without re-inlining (#315 findings §3).
func TestShippedFullSchemasMatchInlineOutput(t *testing.T) {
	matches, err := filepath.Glob(filepath.Join(shippedSchemasDir, "*_full.json"))
	if err != nil {
		t.Fatalf("glob shipped full schemas: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("no *_full.json found under %s — guard is scanning the wrong directory", shippedSchemasDir)
	}
	for _, fullPath := range matches {
		name := strings.TrimSuffix(filepath.Base(fullPath), "_full.json")
		t.Run(name, func(t *testing.T) {
			basePath := filepath.Join(shippedSchemasDir, name+".json")
			inliner := NewSchemaInliner(shippedSchemasDir)
			result, err := inliner.InlineFile(basePath)
			if err != nil {
				t.Fatalf("inline %s: %v", basePath, err)
			}
			// Same encoding as runInlineSchema writes.
			encoded, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				t.Fatalf("marshal inlined schema: %v", err)
			}
			committed, err := os.ReadFile(fullPath)
			if err != nil {
				t.Fatalf("read committed full schema: %v", err)
			}
			if !bytes.Equal(committed, encoded) {
				t.Errorf("%s_full.json does not match inline-schema output (committed %d bytes, inlined %d bytes)\n%s",
					name, len(committed), len(encoded), firstDivergence(committed, encoded))
			}
		})
	}
}
