package lsm

import (
	"testing"

	"github.com/navijation/njsimple/util"
	testing_util "github.com/navijation/njsimple/util/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLSMDB_CUDOperations(t *testing.T) {
	t.Parallel()

	dir, cleanup := testing_util.MkdirTemp(t, "TestCUDOperations")
	cleanup()
	defer cleanup()

	db, err := Open(OpenArgs{
		Path:           dir,
		Create:         true,
		IndexChunkSize: util.Some(uint64(100)),
	})
	require.NoError(t, err)

	require.NoError(t, db.Start())

	require.NoError(t, db.Upsert([]byte("key1"), []byte("value1")))
	require.NoError(t, db.Upsert([]byte("key1"), []byte("value1")))
	require.NoError(t, db.Upsert([]byte("key2"), []byte("value2")))
	require.NoError(t, db.Upsert([]byte("key3"), []byte("value3")))
	require.NoError(t, db.Upsert([]byte("key4"), []byte("value4")))
	require.NoError(t, db.Delete([]byte("key2")))
	require.NoError(t, db.Delete([]byte("key3")))
	require.NoError(t, db.Delete([]byte("key3")))
	require.NoError(t, db.Upsert([]byte("key3"), []byte("value3+")))

	t.Run("lookup entries", func(t *testing.T) {
		_, exists, err := db.Lookup([]byte("nonexistent"))
		_ = assert.NoError(t, err) && assert.False(t, exists)

		entry1, exists, err := db.Lookup([]byte("key1"))
		_ = assert.NoError(t, err) && assert.True(t, exists) && assert.Equal(t, KeyValuePair{
			Key:   []byte("key1"),
			Value: []byte("value1"),
		}, entry1)

		entry2, exists, err := db.Lookup([]byte("key2"))
		_ = assert.NoError(t, err) && assert.True(t, exists) && assert.Equal(t, KeyValuePair{
			Key:       []byte("key2"),
			IsDeleted: true,
		}, entry2)

		entry3, exists, err := db.Lookup([]byte("key3"))
		_ = assert.NoError(t, err) && assert.True(t, exists) && assert.Equal(t, KeyValuePair{
			Key:   []byte("key3"),
			Value: []byte("value3+"),
		}, entry3)

		entry4, exists, err := db.Lookup([]byte("key4"))
		_ = assert.NoError(t, err) && assert.True(t, exists) && assert.Equal(t, KeyValuePair{
			Key:   []byte("key4"),
			Value: []byte("value4"),
		}, entry4)
	})
	require.NoError(t, db.Close())

	sameDB, err := Open(OpenArgs{
		Path:           dir,
		IndexChunkSize: util.Some(uint64(100)),
	})
	require.NoError(t, err)

	require.NoError(t, sameDB.Start())

	t.Run("lookup entries", func(t *testing.T) {
		entry1, exists, err := sameDB.Lookup([]byte("key1"))
		_ = assert.NoError(t, err) && assert.True(t, exists) && assert.Equal(t, KeyValuePair{
			Key:   []byte("key1"),
			Value: []byte("value1"),
		}, entry1)

		entry2, exists, err := sameDB.Lookup([]byte("key2"))
		_ = assert.NoError(t, err) && assert.True(t, exists) && assert.Equal(t, KeyValuePair{
			Key:       []byte("key2"),
			IsDeleted: true,
		}, entry2)

		entry3, exists, err := sameDB.Lookup([]byte("key3"))
		_ = assert.NoError(t, err) && assert.True(t, exists) && assert.Equal(t, KeyValuePair{
			Key:   []byte("key3"),
			Value: []byte("value3+"),
		}, entry3)

		entry4, exists, err := sameDB.Lookup([]byte("key4"))
		_ = assert.NoError(t, err) && assert.True(t, exists) && assert.Equal(t, KeyValuePair{
			Key:   []byte("key4"),
			Value: []byte("value4"),
		}, entry4)
	})
	require.NoError(t, sameDB.Close())
}
