package sstable

import (
	"fmt"
	"testing"

	"github.com/navijation/njsimple/util"
	testing_util "github.com/navijation/njsimple/util/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen_NoEntries(t *testing.T) {
	t.Parallel()

	dir, cleanup := testing_util.MkdirTemp(t, "TestOpen_NewSSTable")
	defer cleanup()

	_, err := Open(OpenArgs{
		Path: dir + "/nonexistent.jrn",
	})
	require.Error(t, err)

	file, err := Open(OpenArgs{
		Path:    dir + "/sstable.sst",
		Create:  true,
		Version: 5,
	})
	require.NoError(t, err)

	assert.Equal(t, uint64(5), file.header.Version)
	assert.NotZero(t, file.header.ID)
	assert.Equal(t, uint64(0), file.header.NumEntries)
	assert.Equal(t, uint64(40), file.header.FileSize)
	assert.Equal(t, defaultChunkSize, file.index.ChunkSize)
	assert.Empty(t, file.index.IndexedEntries)

	assert.NoError(t, file.Close())

	_, err = Open(OpenArgs{
		Path:    dir + "/sstable.sst",
		Create:  true,
		Version: 20,
	})
	assert.Error(t, err, "re-creating an existing file must fail")

	sameFile, err := Open(OpenArgs{
		Path:           dir + "/sstable.sst",
		IndexChunkSize: util.Some(uint64(5)),
	})
	require.NoError(t, err)

	assert.Equal(t, uint64(5), sameFile.header.Version)
	assert.Equal(t, uint64(0), sameFile.header.NumEntries)
	assert.Equal(t, uint64(40), sameFile.header.FileSize)
	assert.Equal(t, file.header.ID, sameFile.header.ID)
	assert.Equal(t, uint64(5), sameFile.index.ChunkSize)
	assert.Empty(t, sameFile.index.IndexedEntries)
}

func TestSSTable_AppendAndIterate(t *testing.T) {
	t.Parallel()

	dir, cleanup := testing_util.MkdirTemp(t, "TestSSTable_AppendAndIterate")
	defer cleanup()

	file, err := Open(OpenArgs{
		Path:           dir + "/sstable.sst",
		Create:         true,
		Version:        5,
		IndexChunkSize: util.Some(uint64(5)),
	})
	require.NoError(t, err)
	defer file.Close()

	err = file.AppendEntries(util.SeqOf(KeyValuePair{
		Key:       []byte("1: Hello world\n"),
		Value:     []byte("Nevermind"),
		IsDeleted: false,
	}))

	require.NoError(t, err)
	entry1, err, exists := util.Seq2At(file.Entries(), 0)
	t.Run("first entry", func(t *testing.T) {
		assert.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, []byte("1: Hello world\n"), entry1.Key)
		assert.Equal(t, []byte("Nevermind"), entry1.Value)
		assert.Equal(t, uint64(15), entry1.KeySize)
		assert.Equal(t, uint64(9), entry1.ValueSize)
		assert.Equal(t, uint64(0), entry1.Location.EntryNumber)
		assert.Equal(t, uint64(40), entry1.Location.Offset)
		assert.False(t, entry1.IsDeleted)

		// 40 + 15 + 9 + 16 = 80
		assert.Equal(t, uint64(80), file.header.FileSize)
		assert.NotZero(t, file.header.ID)
		assert.Equal(t, uint64(1), file.header.NumEntries)
		assert.Equal(t, uint64(5), file.header.Version)
	})

	err = file.AppendEntries(util.SeqOf(KeyValuePair{
		Key:       []byte("2: Goodbye world\n"),
		Value:     []byte("Actually, yeah!"),
		IsDeleted: true,
	}))

	require.NoError(t, err)
	entry2, err, exists := util.Seq2At(file.Entries(), 1)
	t.Run("second entry", func(t *testing.T) {
		require.True(t, exists)
		require.NoError(t, err)
		assert.Equal(t, []byte("2: Goodbye world\n"), entry2.Key)
		assert.Equal(t, []byte("Actually, yeah!"), entry2.Value)
		assert.Equal(t, uint64(17), entry2.KeySize)
		assert.Equal(t, uint64(15), entry2.ValueSize)
		assert.Equal(t, uint64(1), entry2.Location.EntryNumber)
		assert.Equal(t, uint64(80), entry2.Location.Offset)
		assert.True(t, entry2.IsDeleted)

		// 80 + 17 + 15 + 16 = 128
		assert.Equal(t, uint64(128), file.header.FileSize)
		assert.NotZero(t, file.header.ID)
		assert.Equal(t, uint64(2), file.header.NumEntries)
		assert.Equal(t, uint64(5), file.header.Version)
	})

	sameFile, err := Open(OpenArgs{
		Path:           dir + "/sstable.sst",
		IndexChunkSize: util.Some(uint64(5)),
	})
	defer file.Close()
	t.Run("re-open file", func(t *testing.T) {
		require.NoError(t, err)
		assert.Equal(t, file.header, sameFile.header)
		assert.Equal(t, file.index, sameFile.index)
	})

	t.Run("re-opened entries", func(t *testing.T) {
		entry1Copy, err, exists := util.Seq2At(sameFile.Entries(), 0)
		require.True(t, exists)
		require.NoError(t, err)

		assert.Equal(t, entry1, entry1Copy)

		entry2Copy, err, exists := util.Seq2At(sameFile.EntriesAt(entry2.Location), 0)
		require.NoError(t, err)
		require.True(t, exists)
		assert.Equal(t, entry2, entry2Copy)

		// file should not be mutated by iteration
		assert.Equal(t, file.header, sameFile.header)
		assert.Equal(t, file.index, sameFile.index)
	})
}

func TestSSTable_LookupEntry(t *testing.T) {
	t.Parallel()

	dir, cleanup := testing_util.MkdirTemp(t, "TestSSTable_AppendAndIterate")
	defer cleanup()

	file, err := Open(OpenArgs{
		Path:           dir + "/sstable.sst",
		Create:         true,
		Version:        2,
		IndexChunkSize: util.Some(uint64(1000)),
	})
	require.NoError(t, err)
	defer file.Close()

	require.NoError(t, file.AppendEntries(func(yield func(KeyValuePair) bool) {
		for i := range 1000 {
			kvp := KeyValuePair{
				Key:   []byte(fmt.Sprintf("someKey%3d", i)),
				Value: []byte(fmt.Sprintf("someValue%d", i)),
			}
			if !yield(kvp) {
				return
			}
		}
	}))

	assert.LessOrEqual(t, len(file.index.IndexedEntries), int(file.header.FileSize/1000))

	t.Run("lookup existing keys", func(t *testing.T) {
		for i := range 1000 {
			key := []byte(fmt.Sprintf("someKey%3d", i))
			value := []byte(fmt.Sprintf("someValue%d", i))

			entry, exists, err := file.LookupEntry(key)
			assert.NoError(t, err, "%s", key)
			assert.True(t, exists, "%s", key)
			assert.Equal(t, value, entry.Value)
			assert.False(t, entry.IsDeleted)
		}
	})

	t.Run("lookup nonexistent keys", func(t *testing.T) {
		for i := range 1000 {
			key := []byte(fmt.Sprintf("someKey%3dx", i))

			_, exists, err := file.LookupEntry(key)
			assert.NoError(t, err, "%s", key)
			assert.False(t, exists, "%s", key)
		}
	})
}
