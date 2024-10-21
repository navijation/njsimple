package lsm

import (
	"bytes"
	"testing"

	"github.com/navijation/njsimple/storage/journal"
	"github.com/navijation/njsimple/storage/keyvaluepair"
	"github.com/stretchr/testify/assert"
)

func TestCUDKeyValueEntry_Serialization(t *testing.T) {
	t.Parallel()

	t.Run("not deleted", func(t *testing.T) {
		storedKVP := (&keyvaluepair.KeyValuePair{
			Key:   []byte("key1"),
			Value: []byte("value1"),
		}).ToStoredKeyValuePair()

		entry := CUDKeyValueEntry{
			StoredKeyValuePair: storedKVP,
		}

		var buf bytes.Buffer
		_, err := entry.WriteTo(&buf)
		assert.NoError(t, err)

		var deserializedEntry CUDKeyValueEntry
		_, err = deserializedEntry.ReadFrom(&buf)
		assert.NoError(t, err)

		assert.Equal(t, entry, deserializedEntry)
	})

	t.Run("deleted", func(t *testing.T) {
		storedKVP := (&KeyValuePair{
			Key: []byte("key1"),
		}).ToStoredKeyValuePair()

		storedKVP.SetIsDeleted(true)

		entry := CUDKeyValueEntry{
			StoredKeyValuePair: storedKVP,
		}

		var buf bytes.Buffer
		_, err := entry.WriteTo(&buf)
		assert.NoError(t, err)

		var deserializedEntry CUDKeyValueEntry
		_, err = deserializedEntry.ReadFrom(&buf)
		assert.NoError(t, err)

		assert.Equal(t, entry, deserializedEntry)
	})
}

func TestCreateSSTableEntry_Serialization(t *testing.T) {
	t.Parallel()

	tableNumber := uint64(12345)
	entry := CreateSSTableEntry{
		SSTableNumber: tableNumber,
	}

	var buf bytes.Buffer
	_, err := entry.WriteTo(&buf)
	assert.NoError(t, err)

	var deserializedEntry CreateSSTableEntry
	_, err = deserializedEntry.ReadFrom(&buf)
	assert.NoError(t, err)

	assert.Equal(t, entry, deserializedEntry)
}

func TestParseJournalEntry(t *testing.T) {
	t.Parallel()

	t.Run("CUD entry", func(t *testing.T) {
		storedKVP := (&keyvaluepair.KeyValuePair{
			Key:   []byte("key1"),
			Value: []byte("value1"),
		}).ToStoredKeyValuePair()

		cudEntry := CUDKeyValueEntry{StoredKeyValuePair: storedKVP}

		// Serialize CUD entry
		var cudBuf bytes.Buffer
		_, err := cudEntry.WriteTo(&cudBuf)
		assert.NoError(t, err)

		journalEntry := &journal.JournalEntry{Content: cudBuf.Bytes()}
		_, err = parseJournalEntry(journalEntry)
		assert.NoError(t, err)
	})

	t.Run("Unsupported", func(t *testing.T) {
		// Test unsupported entry type
		unsupportedEntry := &journal.JournalEntry{Content: []byte{0xFF}}
		_, err := parseJournalEntry(unsupportedEntry)
		assert.Error(t, err)
	})
}
