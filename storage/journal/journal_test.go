package journal

import (
	"crypto/sha256"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen_NoEntries(t *testing.T) {
	t.Parallel()

	dir := getTemporaryDir(t, "TestOpen_NewJournal")
	defer os.RemoveAll(dir)

	_, err := Open(OpenArgs{
		Path: dir + "/nonexistent.jrn",
	})
	require.Error(t, err)

	file, err := Open(OpenArgs{
		Path:    dir + "/journal.jrn",
		Create:  true,
		StartAt: 5,
	})
	require.NoError(t, err)

	assert.Equal(t, uint64(5), file.header.start)
	assert.NotZero(t, file.header.id)
	assert.Equal(t, uint64(0), file.NumEntries())
	assert.Equal(t, uint64(24), file.Size())
	assert.False(t, file.isBad)
	assert.NotNil(t, file.hash)
	assert.NotEqual(t, sha256.New().Sum(nil), file.hash.Sum(nil))

	assert.NoError(t, file.Close())

	_, err = Open(OpenArgs{
		Path:    dir + "/journal.jrn",
		Create:  true,
		StartAt: 5,
	})
	assert.Error(t, err, "re-creating an existing file must fail")

	sameFile, err := Open(OpenArgs{
		Path: dir + "/journal.jrn",
	})
	require.NoError(t, err)

	assert.Equal(t, file.header, sameFile.header)
	assert.Equal(t, file.numberOfEntries, sameFile.numberOfEntries)
	assert.Equal(t, file.Size(), sameFile.Size())
	assert.Equal(t, file.isBad, sameFile.isBad)
	assert.Equal(t, file.hash.Sum(nil), sameFile.hash.Sum(nil))
}

func TestJournal_AppendAndIterate(t *testing.T) {
	t.Parallel()

	dir := getTemporaryDir(t, "TestJournal_Append")
	defer os.RemoveAll(dir)

	file, err := Open(OpenArgs{
		Path:    dir + "/journal.jrn",
		Create:  true,
		StartAt: 5,
	})
	require.NoError(t, err)

	entry1, err := file.AppendEntry([]byte("Hello world\n"))
	require.NoError(t, err)
	t.Run("first entry", func(t *testing.T) {
		assert.Equal(t, []byte("Hello world\n"), entry1.Content)
		assert.Equal(t, uint64(5), entry1.EntryNumber)
		assert.Equal(t, uint64(24), entry1.Offset)
		// 24 start offset + 8 byte size + 12 byte content + 32 byte signature = 76
		assert.Equal(t, uint64(24+8+12+32), entry1.EndOffset())
		assert.Equal(t, entry1.Signature, file.hash.Sum(nil))

		assert.Equal(t, uint64(5), file.header.start)
		assert.NotZero(t, file.header.id)
		assert.Equal(t, uint64(1), file.numberOfEntries)
		assert.Equal(t, uint64(76), file.Size())
		assert.False(t, file.isBad)
		assert.NotNil(t, file.hash)
	})

	entry2, err := file.AppendEntry([]byte("Goodbye world\n"))
	require.NoError(t, err)
	t.Run("second entry", func(t *testing.T) {
		assert.Equal(t, []byte("Goodbye world\n"), entry2.Content)
		assert.Equal(t, uint64(6), entry2.EntryNumber)
		assert.Equal(t, uint64(76), entry2.Offset)
		// start offset + 8 byte size + 14 byte content + 32 byte signature = 130
		assert.Equal(t, uint64(76+8+14+32), entry2.EndOffset())
		assert.Equal(t, entry2.Signature, file.hash.Sum(nil))

		assert.Equal(t, uint64(5), file.header.start)
		assert.NotZero(t, file.header.id)
		assert.Equal(t, uint64(2), file.numberOfEntries)
		assert.Equal(t, uint64(130), file.Size())
		assert.False(t, file.isBad)
		assert.NotNil(t, file.hash)
	})

	sameFile, err := Open(OpenArgs{
		Path: dir + "/journal.jrn",
	})
	t.Run("re-open file", func(t *testing.T) {
		require.NoError(t, err)
		assert.Equal(t, file.header, sameFile.header)
		assert.Equal(t, file.NumEntries(), sameFile.NumEntries())
		assert.Equal(t, file.Size(), sameFile.Size())
		assert.Equal(t, file.isBad, sameFile.isBad)
		assert.Equal(t, file.hash.Sum(nil), sameFile.hash.Sum(nil))
	})

	t.Run("cursor", func(t *testing.T) {
		cursor := sameFile.NewCursor(true)

		entry1Copy, exists, err := cursor.NextEntry()

		require.NoError(t, err)
		require.True(t, exists)
		assert.Equal(t, entry1, entry1Copy)

		entry2Copy, exists, err := cursor.NextEntry()

		// file should not be mutated by cursor
		assert.Equal(t, file.header, sameFile.header)
		assert.Equal(t, file.numberOfEntries, sameFile.numberOfEntries)
		assert.Equal(t, file.Size(), sameFile.Size())
		assert.Equal(t, file.isBad, sameFile.isBad)
		assert.Equal(t, file.hash.Sum(nil), sameFile.hash.Sum(nil))

		require.NoError(t, err)
		require.True(t, exists)
		assert.Equal(t, entry2, entry2Copy)

		_, exists, err = cursor.NextEntry()
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestJournal_CorruptionHandling(t *testing.T) {
	t.Parallel()

	dir := getTemporaryDir(t, "TestJournal_CorruptionHandling")
	defer os.RemoveAll(dir)

	file, err := Open(OpenArgs{
		Path:    dir + "/journal.jrn",
		Create:  true,
		StartAt: 5,
	})
	require.NoError(t, err)

	_, err = file.AppendEntry([]byte("Hello world\n"))
	require.NoError(t, err)

	hash1 := file.hash.Sum(nil)

	entry2, err := file.AppendEntry([]byte("Goodbye world\n"))
	require.NoError(t, err)

	file.Close()

	t.Run("re-open file after appending garbage", func(t *testing.T) {
		rawFile, err := os.OpenFile(file.path, os.O_RDWR, 0)
		require.NoError(t, err)

		_, err = rawFile.WriteAt([]byte("deadbeef"), int64(file.Size()))
		require.NoError(t, err)

		assert.NoError(t, rawFile.Close())

		sameFile, err := Open(OpenArgs{
			Path: dir + "/journal.jrn",
		})

		require.NoError(t, err)
		assert.Equal(t, file.header, sameFile.header)
		assert.Equal(t, file.numberOfEntries, sameFile.numberOfEntries)
		assert.Equal(t, file.Size(), sameFile.Size())
		assert.Equal(t, file.isBad, sameFile.isBad)
		assert.Equal(t, file.hash.Sum(nil), sameFile.hash.Sum(nil))
	})

	t.Run("re-open file after corrupting signature", func(t *testing.T) {
		rawFile, err := os.OpenFile(file.path, os.O_RDWR, 0)
		require.NoError(t, err)

		_, err = rawFile.WriteAt([]byte("deadbeef"), int64(file.Size()-8))
		require.NoError(t, err)

		assert.NoError(t, rawFile.Close())

		corruptedFile, err := Open(OpenArgs{
			Path: dir + "/journal.jrn",
		})

		require.NoError(t, err)
		assert.Equal(t, file.header, corruptedFile.header)
		assert.Equal(t, file.numberOfEntries-1, corruptedFile.numberOfEntries)
		assert.Equal(t, file.Size()-entry2.SizeOf(), corruptedFile.Size())
		assert.False(t, corruptedFile.isBad)
		assert.Equal(t, hash1, corruptedFile.hash.Sum(nil))
	})
}

func getTemporaryDir(t *testing.T, prefix string) (path string) {
	out, err := os.MkdirTemp(os.TempDir(), prefix)
	if err != nil {
		t.Fatalf("failed to create temporary directory: %v", err)
	}

	return out
}
