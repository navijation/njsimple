package lsm

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/navijation/njsimple/util"
	testing_util "github.com/navijation/njsimple/util/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLSMDB_CreateSSTable(t *testing.T) {
	t.Parallel()

	dir, cleanup := testing_util.MkdirTemp(t, "TestLSMDB_CreateSSTable")
	cleanup()
	defer cleanup()

	const numTestKeyValues = uint64(100)

	db, err := Open(OpenArgs{
		Path:           dir,
		Create:         true,
		IndexChunkSize: util.Some(uint64(1000)),
	})
	require.NoError(t, err)

	require.NoError(t, db.Start())

	require.Len(t, db.writeAheadLogs, 1)

	for i := range numTestKeyValues {
		require.NoError(t, db.Upsert(
			[]byte(fmt.Sprintf("key %03d", i)), []byte(fmt.Sprintf("value %d", i))),
		)
	}

	writeAheadLog := db.writeAheadLogs[0]
	assert.Equal(t, numTestKeyValues, writeAheadLog.NumEntries())

	require.NoError(t, db.CreateSSTable())

	t.Run("assert writeahead log has entry", func(t *testing.T) {
		assert.Equal(t, numTestKeyValues+1, writeAheadLog.NumEntries())

		cursor := writeAheadLog.NewCursor(false)
		for _ = range numTestKeyValues {
			_, exists, err := cursor.NextEntry()
			_ = assert.NoError(t, err) && assert.True(t, exists)
		}

		lastEntry, exists, err := cursor.NextEntry()
		if assert.NoError(t, err) && assert.True(t, exists) {
			entry, err := parseJournalEntry(&lastEntry)
			if assert.NoError(t, err) && assert.IsType(t, CreateSSTableEntry{}, entry) {
				entry := entry.(CreateSSTableEntry)
				assert.EqualValues(t, 1, entry.SSTableNumber)
				assert.EqualValues(t, 2, entry.WriteAheadLogNumber)
				assert.Nil(t, entry.index)
			}
		}
	})

	time.Sleep(500 * time.Millisecond)

	t.Run("Fields after file is committed", func(t *testing.T) {
		if assert.Len(t, db.writeAheadLogs, 1) {
			assert.Zero(t, db.writeAheadLogs[0].NumEntries())
		}
		if assert.Len(t, db.inMemoryIndexes, 1) {
			assert.Empty(t, db.inMemoryIndexes[0].KeyValues)
		}
		if assert.Len(t, db.sstables, 1) {
			assert.Equal(t, numTestKeyValues, db.sstables[0].NumEntries())
		}
		assert.NoError(t, db.stateErr)
	})

	t.Run("Lookup after file is committed", func(t *testing.T) {
		for i := range numTestKeyValues {
			entry, exists, err := db.Lookup([]byte(fmt.Sprintf("key %03d", i)))
			_ = assert.NoError(t, err) && assert.True(t, exists) &&
				assert.Equal(t, []byte(fmt.Sprintf("value %d", i)), entry.Value) &&
				assert.False(t, entry.IsDeleted)
		}
	})

	require.NoError(t, db.Close())

	sameDB, err := Open(OpenArgs{
		Path:           dir,
		Create:         false,
		IndexChunkSize: util.Some(uint64(1000)),
	})
	require.NoError(t, err)

	t.Run("Fields after DB is reopened", func(t *testing.T) {
		if assert.Len(t, sameDB.writeAheadLogs, 1) {
			assert.Zero(t, sameDB.writeAheadLogs[0].NumEntries())
		}
		if assert.Len(t, sameDB.inMemoryIndexes, 1) {
			assert.Empty(t, sameDB.inMemoryIndexes[0].KeyValues)
		}
		if assert.Len(t, sameDB.sstables, 1) {
			assert.Equal(t, numTestKeyValues, sameDB.sstables[0].NumEntries())
		}
		assert.EqualValues(t, 2, sameDB.nextSSTableNumber)
		assert.EqualValues(t, 3, sameDB.nextWriteAheadLogNumber)
		assert.NoError(t, sameDB.stateErr)
	})

	t.Run("Lookup reopened DB", func(t *testing.T) {
		for i := range numTestKeyValues {
			entry, exists, err := sameDB.Lookup([]byte(fmt.Sprintf("key %03d", i)))
			_ = assert.NoError(t, err) && assert.True(t, exists) &&
				assert.Equal(t, []byte(fmt.Sprintf("value %d", i)), entry.Value) &&
				assert.False(t, entry.IsDeleted)
		}
	})
}

func TestLSMDB_ConcurrentSSTableAndCUD(t *testing.T) {
	t.Parallel()

	dir, cleanup := testing_util.MkdirTemp(t, "TestLSMDB_ConcurrentSSTableAndCUD")
	cleanup()
	defer cleanup()

	const numTestKeyValues = uint64(100)

	db, err := Open(OpenArgs{
		Path:           dir,
		Create:         true,
		IndexChunkSize: util.Some(uint64(1000)),
	})
	require.NoError(t, err)

	require.NoError(t, db.Start())

	var (
		writeCounter1 atomic.Int64
		writeCounter2 atomic.Int64
	)

	go func() {
		for i := range numTestKeyValues {
			require.NoError(t, db.Upsert(
				[]byte(fmt.Sprintf("keyX %03d", i)), []byte(fmt.Sprintf("value %d", i))),
			)
			writeCounter1.Add(1)
		}
	}()

	go func() {
		for i := range numTestKeyValues {
			require.NoError(t, db.Upsert(
				[]byte(fmt.Sprintf("keyY %03d", i)), []byte(fmt.Sprintf("value %d", i))),
			)
			writeCounter2.Add(1)
		}
		for i := range numTestKeyValues {
			require.NoError(t, db.Delete([]byte(fmt.Sprintf("keyY %03d", i))))
			writeCounter2.Add(1)
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	// ensure at least a quarter of both data has been written by both goroutines
	for writeCounter1.Load() < int64(numTestKeyValues)/4 ||
		writeCounter2.Load() < int64(numTestKeyValues)/4 {

		if time.Now().After(deadline) {
			require.Failf(t, "Deadline exceeded", "Deadline exceeded on counters %d and %d",
				writeCounter1.Load(), writeCounter2.Load())
		}
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, db.CreateSSTable())

	deadline = time.Now().Add(7 * time.Second)
	for (writeCounter1.Load() + writeCounter2.Load()) < 3*int64(numTestKeyValues) {
		if time.Now().After(deadline) {
			require.Failf(t, "Deadline exceeded", "Deadline exceeded on counters %d and %d",
				writeCounter1.Load(), writeCounter2.Load())
		}
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, db.CreateSSTable())

	time.Sleep(500 * time.Millisecond)

	t.Run("Fields after file is committed", func(t *testing.T) {
		assert.EqualValues(t, 3, db.nextSSTableNumber)
		assert.EqualValues(t, 4, db.nextWriteAheadLogNumber)
		if assert.Len(t, db.inMemoryIndexes, 1) {
			assert.Empty(t, db.inMemoryIndexes[0].KeyValues)
		}
		assert.Len(t, db.writeAheadLogs, 1)
		assert.Len(t, db.sstables, 2)

		assert.True(t, db.isRunning.Load())
		assert.NoError(t, db.stateErr)
	})

	t.Run("Key lookup after file is committed", func(t *testing.T) {
		for i := range numTestKeyValues {
			value := []byte(fmt.Sprintf("value %d", i))

			keyX := []byte(fmt.Sprintf("keyX %03d", i))
			entry, ok, err := db.Lookup(keyX)
			require.NoError(t, err, string(keyX))
			require.True(t, ok, string(keyX))
			require.Equal(t, KeyValuePair{
				Key:   keyX,
				Value: value,
			}, entry, string(keyX))

			keyY := []byte(fmt.Sprintf("keyY %03d", i))
			entry, ok, err = db.Lookup(
				[]byte(fmt.Sprintf("keyY %03d", i)),
			)
			require.NoError(t, err, string(keyY))
			require.True(t, ok, string(keyY))
			require.Equal(t, KeyValuePair{
				Key:       keyY,
				IsDeleted: true,
			}, entry, string(keyY))
		}
	})
	db.Close()

	sameDB, err := Open(OpenArgs{
		Path:           dir,
		IndexChunkSize: util.Some(uint64(1000)),
	})
	require.NoError(t, err)

	require.NoError(t, sameDB.Start())

	t.Run("Fields after file is committed", func(t *testing.T) {
		assert.EqualValues(t, 3, sameDB.nextSSTableNumber)
		assert.EqualValues(t, 4, sameDB.nextWriteAheadLogNumber)
		if assert.Len(t, sameDB.inMemoryIndexes, 1) {
			assert.Empty(t, sameDB.inMemoryIndexes[0].KeyValues)
		}
		assert.Len(t, sameDB.writeAheadLogs, 1)
		assert.Len(t, sameDB.sstables, 2)

		assert.True(t, sameDB.isRunning.Load())
		assert.NoError(t, sameDB.stateErr)
	})

	t.Run("Key lookup after file is committed", func(t *testing.T) {
		for i := range numTestKeyValues {
			value := []byte(fmt.Sprintf("value %d", i))

			keyX := []byte(fmt.Sprintf("keyX %03d", i))
			entry, ok, err := sameDB.Lookup(keyX)
			require.NoError(t, err, string(keyX))
			require.True(t, ok, string(keyX))
			require.Equal(t, KeyValuePair{
				Key:   keyX,
				Value: value,
			}, entry, string(keyX))

			keyY := []byte(fmt.Sprintf("keyY %03d", i))
			entry, ok, err = sameDB.Lookup(
				[]byte(fmt.Sprintf("keyY %03d", i)),
			)
			require.NoError(t, err, string(keyY))
			require.True(t, ok, string(keyY))
			require.Equal(t, KeyValuePair{
				Key:       keyY,
				IsDeleted: true,
			}, entry, string(keyY))
		}
	})
}
