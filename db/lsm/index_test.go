package lsm

import (
	"fmt"
	"testing"

	"github.com/navijation/njsimple/storage/keyvaluepair"
	"github.com/stretchr/testify/assert"
)

func TestInMemoryIndex_Upsert(t *testing.T) {
	t.Parallel()

	index := InMemoryIndex{}

	kvp1 := keyvaluepair.KeyValuePair{Key: []byte("key1"), Value: []byte("value1")}
	kvp2 := keyvaluepair.KeyValuePair{Key: []byte("key2"), Value: []byte("value2")}
	kvp1Plus := keyvaluepair.KeyValuePair{Key: []byte("key1"), Value: []byte("new_value1")}

	index.Upsert(kvp1)
	index.Upsert(kvp2)

	if assert.Equal(t, 2, len(index.KeyValues)) {
		assert.Equal(t, kvp1, index.KeyValues[0])
		assert.Equal(t, kvp2, index.KeyValues[1])
	}

	index.Upsert(kvp1Plus)

	if assert.Equal(t, 2, len(index.KeyValues)) {
		assert.Equal(t, kvp1Plus, index.KeyValues[0])
		assert.Equal(t, kvp2, index.KeyValues[1])
	}

	for i := 100; i > 2; i-- {
		index.Upsert(KeyValuePair{
			Key:       []byte(fmt.Sprintf("key%d", i)),
			Value:     []byte(fmt.Sprintf("value%d", i)),
			IsDeleted: false,
		})
	}

	assert.Len(t, index.KeyValues, 100)
}

func TestInMemoryIndex_Lookup(t *testing.T) {
	t.Parallel()

	index := InMemoryIndex{}

	kvp1 := keyvaluepair.KeyValuePair{Key: []byte("key1"), Value: []byte("value1")}
	kvp2 := keyvaluepair.KeyValuePair{Key: []byte("key2"), Value: []byte("value2")}

	index.Upsert(kvp1)
	index.Upsert(kvp2)

	// Test lookup for existing key
	result, exists := index.Lookup([]byte("key1"))
	assert.True(t, exists)
	assert.Equal(t, kvp1, result)

	// Test lookup for non-existing key
	_, exists = index.Lookup([]byte("key3"))
	assert.False(t, exists)
}
