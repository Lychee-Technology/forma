package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSetAdd tests adding items to a set
func TestSetAdd(t *testing.T) {
	set := NewSet[int]()
	set.Add(1)
	set.Add(2)
	set.Add(3)

	assert.Equal(t, 3, set.Size())
	assert.True(t, set.Contains(1))
	assert.True(t, set.Contains(2))
	assert.True(t, set.Contains(3))
	assert.False(t, set.Contains(4))
}

// TestSetAddDuplicate tests that adding duplicate items doesn't increase size
func TestSetAddDuplicate(t *testing.T) {
	set := NewSet[string]()
	set.Add("apple")
	set.Add("apple")
	set.Add("apple")

	assert.Equal(t, 1, set.Size())
	assert.True(t, set.Contains("apple"))
}

// TestSetRemove tests removing items from a set
func TestSetRemove(t *testing.T) {
	set := NewSet[int]()
	set.Add(1)
	set.Add(2)
	set.Add(3)

	set.Remove(2)

	assert.Equal(t, 2, set.Size())
	assert.True(t, set.Contains(1))
	assert.False(t, set.Contains(2))
	assert.True(t, set.Contains(3))
}

// TestSetRemoveNonExistent tests removing an item that doesn't exist
func TestSetRemoveNonExistent(t *testing.T) {
	set := NewSet[int]()
	set.Add(1)

	set.Remove(2)

	assert.Equal(t, 1, set.Size())
	assert.True(t, set.Contains(1))
}

// TestSetContains tests checking if items exist in the set
func TestSetContains(t *testing.T) {
	set := NewSet[string]()
	set.Add("hello")

	assert.True(t, set.Contains("hello"))
	assert.False(t, set.Contains("world"))
}

// TestSetSize tests getting the size of a set
func TestSetSize(t *testing.T) {
	set := NewSet[int]()

	assert.Equal(t, 0, set.Size())

	set.Add(1)
	assert.Equal(t, 1, set.Size())

	set.Add(2)
	set.Add(3)
	assert.Equal(t, 3, set.Size())

	set.Remove(2)
	assert.Equal(t, 2, set.Size())
}

// TestSetToSlice tests converting a set to a slice
func TestSetToSlice(t *testing.T) {
	set := NewSet[int]()
	set.Add(1)
	set.Add(2)
	set.Add(3)

	slice := set.ToSlice()

	assert.Equal(t, 3, len(slice))
	assert.Contains(t, slice, 1)
	assert.Contains(t, slice, 2)
	assert.Contains(t, slice, 3)
}

// TestSetToSliceEmpty tests converting an empty set to a slice
func TestSetToSliceEmpty(t *testing.T) {
	set := NewSet[string]()

	slice := set.ToSlice()

	assert.Equal(t, 0, len(slice))
	assert.NotNil(t, slice)
}

// TestSetClear tests clearing all items from a set
func TestSetClear(t *testing.T) {
	set := NewSet[int]()
	set.Add(1)
	set.Add(2)
	set.Add(3)

	assert.Equal(t, 3, set.Size())

	set.Clear()

	assert.Equal(t, 0, set.Size())
	assert.False(t, set.Contains(1))
	assert.False(t, set.Contains(2))
	assert.False(t, set.Contains(3))
}

// TestSetWithStringType tests Set with string type
func TestSetWithStringType(t *testing.T) {
	set := NewSet[string]()
	set.Add("apple")
	set.Add("banana")
	set.Add("cherry")

	assert.Equal(t, 3, set.Size())
	assert.True(t, set.Contains("apple"))
	assert.True(t, set.Contains("banana"))
	assert.True(t, set.Contains("cherry"))
	assert.False(t, set.Contains("grape"))
}

// TestMapKeys tests extracting keys from a map
func TestMapKeys(t *testing.T) {
	m := map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
	}

	keys := MapKeys(m)

	assert.Equal(t, 3, len(keys))
	assert.Contains(t, keys, "a")
	assert.Contains(t, keys, "b")
	assert.Contains(t, keys, "c")
}

// TestMapKeysEmpty tests extracting keys from an empty map
func TestMapKeysEmpty(t *testing.T) {
	m := map[int]string{}

	keys := MapKeys(m)

	assert.Equal(t, 0, len(keys))
	assert.NotNil(t, keys)
}

// TestMapKeysNil tests extracting keys from a nil map
func TestMapKeysNil(t *testing.T) {
	var m map[string]int

	keys := MapKeys(m)

	assert.Equal(t, 0, len(keys))
	assert.NotNil(t, keys)
}

// TestMapKeysIntKeys tests extracting integer keys from a map
func TestMapKeysIntKeys(t *testing.T) {
	m := map[int]string{
		1: "one",
		2: "two",
		3: "three",
	}

	keys := MapKeys(m)

	assert.Equal(t, 3, len(keys))
	assert.Contains(t, keys, 1)
	assert.Contains(t, keys, 2)
	assert.Contains(t, keys, 3)
}

// TestMapValues tests extracting values from a map
func TestMapValues(t *testing.T) {
	m := map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
	}

	values := MapValues(m)

	assert.Equal(t, 3, len(values))
	assert.Contains(t, values, 1)
	assert.Contains(t, values, 2)
	assert.Contains(t, values, 3)
}

// TestMapValuesEmpty tests extracting values from an empty map
func TestMapValuesEmpty(t *testing.T) {
	m := map[int]string{}

	values := MapValues(m)

	assert.Equal(t, 0, len(values))
	assert.NotNil(t, values)
}

// TestMapValuesNil tests extracting values from a nil map
func TestMapValuesNil(t *testing.T) {
	var m map[string]int

	values := MapValues(m)

	assert.Equal(t, 0, len(values))
	assert.NotNil(t, values)
}

// TestMapValuesStringValues tests extracting string values from a map
func TestMapValuesStringValues(t *testing.T) {
	m := map[int]string{
		1: "one",
		2: "two",
		3: "three",
	}

	values := MapValues(m)

	assert.Equal(t, 3, len(values))
	assert.Contains(t, values, "one")
	assert.Contains(t, values, "two")
	assert.Contains(t, values, "three")
}

// TestMapKeysAndValuesConsistency tests that keys and values are consistent
func TestMapKeysAndValuesConsistency(t *testing.T) {
	m := map[string]int{
		"x": 10,
		"y": 20,
		"z": 30,
	}

	keys := MapKeys(m)
	values := MapValues(m)

	assert.Equal(t, len(keys), len(values))
	assert.Equal(t, len(keys), len(m))
}
