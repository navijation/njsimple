package lsm

import (
	"testing"

	"github.com/navijation/njsimple/util"
	testing_util "github.com/navijation/njsimple/util/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	t.Parallel()

	dir, cleanup := testing_util.MkdirTemp(t, "TestOpen")
	cleanup()
	defer cleanup()

	db, err := Open(OpenArgs{
		Path:           dir,
		Create:         true,
		IndexChunkSize: util.Some(uint64(100)),
	})
	require.NoError(t, err)

	t.Run("create new DB", func(t *testing.T) {
		assert.NotNil(t, db.asyncEntryChan)
		assert.NotNil(t, db.done)

		assert.Equal(t, uint64(2), db.nextWriteAheadLogNumber)
		assert.Equal(t, uint64(1), db.nextSSTableNumber)
		assert.Equal(t, []*InMemoryIndex{{}}, db.inMemoryIndexes)
		if assert.Len(t, db.writeAheadLogs, 1) {
			assert.Equal(t, dir+"/writeahead_log_1.jrn", db.writeAheadLogs[0].Path())
			assert.FileExists(t, db.writeAheadLogs[0].Path())
		}
		assert.Len(t, db.sstables, 0)
		assert.NoError(t, db.stateErr)
		assert.False(t, db.isRunning.Load())
		assert.Equal(t, dir, db.path)
	})

	require.NoError(t, db.Close())

	sameDB, err := Open(OpenArgs{
		Path:           dir,
		IndexChunkSize: util.Some(uint64(100)),
	})
	require.NoError(t, err)

	t.Run("open existing DB", func(t *testing.T) {
		assert.NotNil(t, sameDB.asyncEntryChan)
		assert.NotNil(t, sameDB.done)

		assert.Equal(t, uint64(2), sameDB.nextWriteAheadLogNumber)
		assert.Equal(t, uint64(1), sameDB.nextSSTableNumber)
		assert.Equal(t, []*InMemoryIndex{{}}, sameDB.inMemoryIndexes)
		if assert.Len(t, db.writeAheadLogs, 1) {
			assert.Equal(t, dir+"/writeahead_log_1.jrn", db.writeAheadLogs[0].Path())
			assert.FileExists(t, db.writeAheadLogs[0].Path())
		}
		assert.Len(t, sameDB.sstables, 0)
		assert.NoError(t, sameDB.stateErr)
		assert.False(t, sameDB.isRunning.Load())
		assert.Equal(t, dir, sameDB.path)
	})

	require.NoError(t, sameDB.Close())
}

func TestLSMDB_StartClose(t *testing.T) {
	t.Parallel()

	dir, cleanup := testing_util.MkdirTemp(t, "TestLSMDB_StartClose")
	cleanup()
	defer cleanup()

	db, err := Open(OpenArgs{
		Path:           dir,
		Create:         true,
		IndexChunkSize: util.Some(uint64(100)),
	})
	require.NoError(t, err)

	require.NoError(t, db.Start())
	require.NoError(t, db.Close())
}
