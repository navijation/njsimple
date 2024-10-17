package lsm

import (
	"bytes"
	"slices"

	"github.com/navijation/njsimple/storage/keyvaluepair"
)

// TODO: replace this with a binary search tree for added realism
type InMemoryIndex struct {
	KeyValues []keyvaluepair.KeyValuePair
}

func (me *InMemoryIndex) Upsert(kvp keyvaluepair.KeyValuePair) {
	idx, exists := slices.BinarySearchFunc(
		me.KeyValues, kvp.Key, func(pair keyvaluepair.KeyValuePair, target []byte) int {
			return bytes.Compare(pair.Key, target)
		},
	)
	if exists {
		me.KeyValues[idx] = kvp
	} else {
		me.KeyValues = slices.Insert(me.KeyValues, idx, kvp)
	}
}

func (me *InMemoryIndex) Lookup(key []byte) (out keyvaluepair.KeyValuePair, exists bool) {
	idx, exists := slices.BinarySearchFunc(
		me.KeyValues, key, func(pair keyvaluepair.KeyValuePair, target []byte) int {
			return bytes.Compare(pair.Key, target)
		},
	)
	if !exists {
		return out, false
	}

	return me.KeyValues[idx], true
}
