package sstable

import (
	"bytes"
	"slices"
)

type SparseMemIndex struct {
	ChunkSize      uint64
	IndexedEntries []SparseMemIndexEntry
}

type SparseMemIndexEntry struct {
	Key      []byte
	Location EntryLocation
}

// Return the location within the SSTable file to start searching for a key, using binary search
func (me *SparseMemIndex) LookupSearchLocation(key []byte) EntryLocation {
	index, exists := slices.BinarySearchFunc(
		me.IndexedEntries, key, func(entry SparseMemIndexEntry, key []byte) int {
			return bytes.Compare(entry.Key, key)
		},
	)
	if exists {
		return me.IndexedEntries[index].Location
	}
	if index == 0 {
		return EntryLocation{}
	}
	return me.IndexedEntries[index-1].Location
}
