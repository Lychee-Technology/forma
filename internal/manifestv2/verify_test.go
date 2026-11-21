package manifestv2

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/stretchr/testify/require"
)

// reseal rewrites the document raw holds and answers the new bytes with ref
// naming their digest: a manifest whose digest matches what PostgreSQL
// committed, so only the full-document checks can refuse it.
func reseal(t *testing.T, raw []byte, ref Ref, edit func(doc map[string]any)) ([]byte, Ref) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var doc map[string]any
	require.NoError(t, dec.Decode(&doc))
	edit(doc)
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	ref.ManifestSHA256 = Digest(out)
	return out, ref
}

func firstFile(doc map[string]any) map[string]any {
	return doc["files"].([]any)[0].(map[string]any)
}

func requireInvalid(t *testing.T, err error, ref Ref) {
	t.Helper()
	require.ErrorIs(t, err, publication.ErrManifestInvalid)
	var ce *ConsistencyError
	require.ErrorAs(t, err, &ce)
	require.Equal(t, ConsistencyError{SchemaID: ref.SchemaID, Epoch: ref.Epoch, Generation: ref.Generation, ManifestKey: ref.ManifestKey, Err: ce.Err}, *ce)
}

func TestVerifyRefusesBytesPostgreSQLDidNotCommit(t *testing.T) {
	raw, ref := encoded(t, testManifest(t))

	flipped := bytes.Clone(raw)
	flipped[bytes.Index(flipped, []byte(`"row_count":10`))+len(`"row_count":1`)] = '9'
	cases := map[string]func() ([]byte, Ref){
		"a flipped byte": func() ([]byte, Ref) { return flipped, ref },
		"another digest": func() ([]byte, Ref) { r := ref; r.ManifestSHA256 = Digest([]byte("x")); return raw, r },
		"an unknown format": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { d["format"] = "forma-manifest/3" })
		},
		"another generation": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { d["generation"] = 8 })
		},
		"another token": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { d["token"] = uuid.NewString() })
		},
		"another epoch":  func() ([]byte, Ref) { return reseal(t, raw, ref, func(d map[string]any) { d["epoch"] = 2 }) },
		"another schema": func() ([]byte, Ref) { return reseal(t, raw, ref, func(d map[string]any) { d["schema_id"] = 101 }) },
		"another parent": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { d["parent_generation"] = 5 })
		},
		"another lineage": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { d["lineage"] = uuid.NewString() })
		},
		"another kind": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { d["kind"] = "compact"; d["input_generation"] = 5 })
		},
		"an absent parent": func() ([]byte, Ref) {
			r := ref
			r.ParentGeneration = 0
			return reseal(t, raw, r, func(d map[string]any) { delete(d, "parent_generation") })
		},
		"a null lineage": func() ([]byte, Ref) {
			r := ref
			r.Lineage = uuid.Nil
			return reseal(t, raw, r, func(d map[string]any) { d["lineage"] = nil })
		},
		"an absent file field": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { delete(firstFile(d), "columns") })
		},
		"an unknown field": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { d["root"] = "manifest.json" })
		},
		"an unknown file field": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { firstFile(d)["checksum"] = "x" })
		},
		"a forbidden-empty flush": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { d["files"] = []any{} })
		},
		"a file without a key": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { firstFile(d)["key"] = "" })
		},
		"a file under another prefix": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) {
				firstFile(d)["key"] = Layout{Prefix: "other"}.ParquetKey(100, 1, TierBase, uuid.MustParse("0190a000-0000-7000-8000-000000000001"))
			})
		},
		"a manifest key of another generation": func() ([]byte, Ref) {
			r := ref
			r.ManifestKey = testLayout.ManifestKey(100, 1, 8, testToken)
			return raw, r
		},
		"a manifest key off the layout": func() ([]byte, Ref) { r := ref; r.ManifestKey = "data/100/manifest.json"; return raw, r },
		"trailing data": func() ([]byte, Ref) {
			out := append(bytes.Clone(raw), '}')
			r := ref
			r.ManifestSHA256 = Digest(out)
			return out, r
		},
		"not json": func() ([]byte, Ref) { r := ref; r.ManifestSHA256 = Digest([]byte("{")); return []byte("{"), r },
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			raw, r := build()
			m, err := Verify(raw, r)
			require.Nil(t, m)
			requireInvalid(t, err, r)
		})
	}
}

// edited answers raw with old replaced, once, by new, and ref naming the
// result's digest: a byte-level edit no map re-encoding can make (a member
// named twice, a case-folded name), sealed as PostgreSQL would have it.
func edited(t *testing.T, raw []byte, ref Ref, old, new string) ([]byte, Ref) {
	t.Helper()
	require.Contains(t, string(raw), old)
	out := bytes.Replace(raw, []byte(old), []byte(new), 1)
	ref.ManifestSHA256 = Digest(out)
	return out, ref
}

// TestVerifyRefusesMembersOutsideTheWireFormat pins the exact wire format:
// every case carries the digest PostgreSQL committed and decodes, under
// encoding/json, to the very manifest the fixture is, so only the member
// names and their presence tell it apart.
func TestVerifyRefusesMembersOutsideTheWireFormat(t *testing.T) {
	raw, ref := encoded(t, testManifest(t))
	cases := map[string]func() ([]byte, Ref){
		"input_generation 0 on a flush": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { d["input_generation"] = 0 })
		},
		"a null input_generation on a flush": func() ([]byte, Ref) {
			return reseal(t, raw, ref, func(d map[string]any) { d["input_generation"] = nil })
		},
		"a manifest member named twice": func() ([]byte, Ref) {
			return edited(t, raw, ref, `"epoch":1,`, `"epoch":1,"epoch":1,`)
		},
		"a file member named twice": func() ([]byte, Ref) {
			return edited(t, raw, ref, `"row_count":10,`, `"row_count":10,"row_count":10,`)
		},
		"a column named twice": func() ([]byte, Ref) {
			return edited(t, raw, ref, `"name":"VARCHAR"`, `"name":"VARCHAR","name":"VARCHAR"`)
		},
		"a case-folded alias beside a member": func() ([]byte, Ref) {
			return edited(t, raw, ref, `"kind":"flush"`, `"kind":"flush","KIND":"flush"`)
		},
		"a case-folded alias of a file member": func() ([]byte, Ref) {
			return edited(t, raw, ref, `"row_count":10,`, `"row_count":10,"Row_Count":10,`)
		},
		"a case-folded spelling of a member": func() ([]byte, Ref) {
			return edited(t, raw, ref, `"parent_generation":6`, `"Parent_Generation":6`)
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			raw, r := build()
			m, err := Verify(raw, r)
			require.Nil(t, m)
			requireInvalid(t, err, r)
		})
	}

	compact := testManifest(t)
	compact.Kind, compact.InputGeneration = publication.KindCompact, 5
	raw, ref = encoded(t, compact)
	m, err := Verify(raw, ref)
	require.NoError(t, err, "a compact manifest names its input_generation")
	require.Equal(t, int64(5), m.InputGeneration)
	for name, edit := range map[string]func(d map[string]any){
		"absent": func(d map[string]any) { delete(d, "input_generation") },
		"null":   func(d map[string]any) { d["input_generation"] = nil },
	} {
		bad, r := reseal(t, raw, ref, edit)
		_, err := Verify(bad, r)
		requireInvalid(t, err, r)
		require.ErrorContains(t, err, "input_generation", "a compact manifest's input_generation is %s", name)
	}
}

func TestVerifyChecksLineageAndKindWhenTheReferenceNamesThem(t *testing.T) {
	raw, ref := encoded(t, testManifest(t))
	rec, err := RecordRef(publication.Record{
		SchemaID: 100, Epoch: 1, State: publication.Published, Generation: 7, ParentGeneration: 6,
		ManifestKey: ref.ManifestKey, ManifestSHA256: ref.ManifestSHA256, Lineage: testLineage, Token: testToken,
	})
	require.NoError(t, err)
	require.Empty(t, rec.Kind, "a record does not name its generation's kind")
	_, err = Verify(raw, rec)
	require.NoError(t, err)

	hist := GenerationRef(100, publication.Generation{
		Generation: 7, Epoch: 1, Kind: publication.KindFlush, ParentGeneration: 6,
		ManifestKey: ref.ManifestKey, ManifestSHA256: ref.ManifestSHA256, Lineage: testLineage, Token: testToken,
	})
	require.Equal(t, testLineage, hist.Lineage)
	_, err = Verify(raw, hist)
	require.NoError(t, err)
	wrongLineage := hist
	wrongLineage.Lineage = uuid.New()
	_, err = Verify(raw, wrongLineage)
	requireInvalid(t, err, wrongLineage)
	hist.Kind = publication.KindInit
	_, err = Verify(raw, hist)
	requireInvalid(t, err, hist)

	_, err = RecordRef(publication.Record{SchemaID: 100, Epoch: 1, State: publication.Unpublished})
	require.ErrorIs(t, err, errNotPublished)
}

// A flush publishes its marked rows in at least one new delta object, so a
// committed flush manifest that lists no file is refused as invalid however
// well it is sealed, and Encode refuses to write one. An init of no
// rows is the one empty generation the protocol has, and it verifies.
func TestVerifyEnforcesTheFlushInventoryButAcceptsAnEmptyInit(t *testing.T) {
	empty := testManifest(t)
	empty.Files = []File{}
	_, _, err := empty.Encode()
	require.ErrorContains(t, err, "generation lists at least one file")

	init := testManifest(t)
	init.Kind = publication.KindInit
	init.Files = []File{}
	raw, ref := encoded(t, init)
	m, err := Verify(raw, ref)
	require.NoError(t, err)
	require.Empty(t, m.Files)
	require.Equal(t, publication.KindInit, m.Kind)
}
