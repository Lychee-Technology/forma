// Package schemaledger records which declaration of each schema is active
// (D2, #710 Phase 2, #733).
//
// A generation is a per-schema monotonic number plus a content hash over the
// schema's JSON Schema file and its attributes file. The ledger keeps, per
// generation and attribute, what the data plane depends on: the attribute id
// and name, its valueType and items_type, and its main-column binding. The
// active generation is a column on the schema registry table.
//
// Only the control plane writes it: init-db records generation 1 of a schema
// it registers, baseline-schema seeds an existing schema after auditing its
// stored data, and apply-schema records each later edit that the type rules
// (Judge) allow. The data plane only reads it: a process refuses to start
// unless every attribute it declares is in the active generation with the
// same type, items_type and binding (Contained).
package schemaledger

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/lychee-technology/forma"
)

// Entry is one attribute as a generation records it.
type Entry struct {
	AttrID    int16
	Name      string
	ValueType forma.ValueType
	// ItemsType is a list's effective element type, and empty for a scalar.
	ItemsType forma.ValueType
	// Column and Encoding are the main-column binding, both empty for an
	// EAV-only attribute. Encoding is never empty for a bound attribute: the
	// attributes file's omitted encoding is recorded as default.
	Column   forma.MainColumn
	Encoding forma.MainColumnEncoding
	// Retired marks an attribute the schema no longer declares (#342). A
	// removed attribute keeps its entry in every later generation.
	Retired bool
}

// EntryOf records meta.
func EntryOf(meta forma.AttributeMetadata) Entry {
	e := Entry{
		AttrID:    meta.AttributeID,
		Name:      meta.AttributeName,
		ValueType: meta.ValueType,
		Retired:   meta.Retired,
	}
	if meta.ValueType == forma.ValueTypeList {
		e.ItemsType = meta.EffectiveItemsType()
	}
	if b := meta.ColumnBinding; b != nil {
		e.Column = b.ColumnName
		e.Encoding = b.Encoding
		if e.Encoding == "" {
			e.Encoding = forma.MainColumnEncodingDefault
		}
	}
	return e
}

// EntriesOf records every attribute of a full attributes file, retired ones
// included, ordered by attribute id.
func EntriesOf(cache forma.SchemaAttributeCache) []Entry {
	entries := make([]Entry, 0, len(cache))
	for name, meta := range cache {
		if meta.AttributeName == "" {
			meta.AttributeName = name
		}
		entries = append(entries, EntryOf(meta))
	}
	sortEntries(entries)
	return entries
}

func sortEntries(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].AttrID < entries[j].AttrID })
}

// typeLabel names the entry's type as the attributes file spells it.
func (e Entry) typeLabel() string {
	if e.ValueType == forma.ValueTypeList {
		return fmt.Sprintf("list<%s>", e.ItemsType)
	}
	return string(e.ValueType)
}

// storageLabel names where the entry is stored.
func (e Entry) storageLabel() string {
	if e.Column == "" {
		return "EAV-only"
	}
	if e.Encoding == forma.MainColumnEncodingDefault {
		return "bound to " + string(e.Column)
	}
	return fmt.Sprintf("bound to %s (%s)", e.Column, e.Encoding)
}

// describe names the entry's type and storage, as diagnostics compare them.
func (e Entry) describe() string { return e.typeLabel() + " " + e.storageLabel() }

func (e Entry) sameType(o Entry) bool {
	return e.ValueType == o.ValueType && e.ItemsType == o.ItemsType
}

func (e Entry) sameBinding(o Entry) bool { return e.Column == o.Column && e.Encoding == o.Encoding }

// columnType is the type of the bound column, and empty when EAV-only.
func (e Entry) columnType() forma.MainColumnType {
	if e.Column == "" {
		return ""
	}
	binding := forma.MainColumnBinding{ColumnName: e.Column}
	return binding.ColumnType()
}

// Declaration is a schema's declaration as read from its files.
type Declaration struct {
	SchemaName string
	SchemaID   int16
	// Hash is ContentHash over the files the entries were read from.
	Hash    string
	Entries []Entry
}

// ContentHash hashes a schema's two files. A missing JSON Schema file hashes
// as empty, which the registry accepts too. Each file is length-prefixed, so
// moving bytes from one file to the other changes the hash.
func ContentHash(schemaDoc, attributesDoc []byte) string {
	h := sha256.New()
	for _, doc := range [][]byte{schemaDoc, attributesDoc} {
		fmt.Fprintf(h, "%d:", len(doc))
		h.Write(doc)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Generation is a recorded generation of one schema.
type Generation struct {
	SchemaID int16
	Number   int64
	Hash     string
	Entries  []Entry
}

func (g Generation) entryByID() map[int16]Entry {
	byID := make(map[int16]Entry, len(g.Entries))
	for _, e := range g.Entries {
		byID[e.AttrID] = e
	}
	return byID
}
