package internal

// Set is a generic data structure that represents a collection of unique items.
// It uses a map internally for O(1) operations.
type Set[T comparable] struct {
	items map[T]struct{}
}

// NewSet creates and returns a new empty Set.
func NewSet[T comparable]() *Set[T] {
	return &Set[T]{
		items: make(map[T]struct{}),
	}
}

// Add inserts an item into the set. If the item already exists, it has no effect.
func (s *Set[T]) Add(item T) {
	s.items[item] = struct{}{}
}

// Remove deletes an item from the set. If the item doesn't exist, it has no effect.
func (s *Set[T]) Remove(item T) {
	delete(s.items, item)
}

// Contains checks if an item exists in the set.
func (s *Set[T]) Contains(item T) bool {
	_, exists := s.items[item]
	return exists
}

// Size returns the number of items in the set.
func (s *Set[T]) Size() int {
	return len(s.items)
}

// ToSlice converts the set to a slice containing all items.
// The order of items is non-deterministic due to map iteration.
func (s *Set[T]) ToSlice() []T {
	slice := make([]T, 0, len(s.items))
	for item := range s.items {
		slice = append(slice, item)
	}
	return slice
}

// Clear removes all items from the set.
func (s *Set[T]) Clear() {
	s.items = make(map[T]struct{})
}

// MapKeys extracts all keys from a map and returns them as a slice.
// The order of keys is non-deterministic due to map iteration.
func MapKeys[K comparable, V any](m map[K]V) []K {
	if m == nil {
		return []K{}
	}
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// MapValues extracts all values from a map and returns them as a slice.
// The order of values is non-deterministic due to map iteration.
func MapValues[K comparable, V any](m map[K]V) []V {
	if m == nil {
		return []V{}
	}
	values := make([]V, 0, len(m))
	for _, value := range m {
		values = append(values, value)
	}
	return values
}
