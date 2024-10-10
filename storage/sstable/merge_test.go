package sstable

import (
	"fmt"
	"os"
	"testing"

	"github.com/navijation/njsimple/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTable_Merge(t *testing.T) {
	dir := getTemporaryDir(t, "TestSSTable_Merge")
	defer os.RemoveAll(dir)

	t.Run("no tables", func(t *testing.T) {
		dst, err := Open(OpenArgs{
			Path:    dir + "/no_tables.sst",
			Create:  true,
			Version: 5,
		})
		require.NoError(t, err)
		defer dst.Close()

		err = dst.MergeTables(MergeTablesArgs{})
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), dst.header.NumEntries)
	})

	t.Run("one table", func(t *testing.T) {
		dst, err := Open(OpenArgs{
			Path:    dir + "/one_table.sst",
			Create:  true,
			Version: 5,
		})
		require.NoError(t, err)
		defer dst.Close()

		src, err := Open(OpenArgs{
			Path:           dir + "/one_table_1.sst",
			Create:         true,
			Version:        5,
			IndexChunkSize: util.Some(uint64(25)),
		})
		require.NoError(t, err)
		defer src.Close()

		require.NoError(t, src.AppendEntries(func(yield func(KeyValuePair) bool) {
			for i := range 100 {
				if !yield(KeyValuePair{
					Key:   []byte(fmt.Sprintf("%03d", i)),
					Value: []byte("src"),
				}) {
					return
				}
			}
		}))

		err = dst.MergeTables(MergeTablesArgs{Srcs: []*SSTable{&src}})
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), dst.header.NumEntries)

		middleEntry, err, exists := util.Seq2At(dst.Entries(), 50)
		if assert.NoError(t, err) && assert.True(t, exists) {
			assert.Equal(t, []byte("050"), middleEntry.Key)
			assert.Equal(t, []byte("src"), middleEntry.Value)
			assert.False(t, middleEntry.IsDeleted)
		}

		assert.Equal(t, uint64(5), dst.header.Version)
		assert.Equal(t, defaultChunkSize, dst.index.ChunkSize)
	})

	t.Run("two tables large", func(t *testing.T) {
		dst, err := Open(OpenArgs{
			Path:    dir + "/two_tables.sst",
			Create:  true,
			Version: 5,
		})
		require.NoError(t, err)
		defer dst.Close()

		src1, err := Open(OpenArgs{
			Path:    dir + "/two_tables_1.sst",
			Create:  true,
			Version: 5,
		})
		require.NoError(t, err)
		defer src1.Close()

		src2, err := Open(OpenArgs{
			Path:    dir + "/two_tables_2.sst",
			Create:  true,
			Version: 5,
		})
		require.NoError(t, err)
		defer src2.Close()

		require.NoError(t, src1.AppendEntries(func(yield func(KeyValuePair) bool) {
			for i := range 150 {
				if i%2 == 0 {
					continue
				}
				if !yield(KeyValuePair{
					Key:   []byte(fmt.Sprintf("%03d", i)),
					Value: []byte("src1"),
				}) {
					return
				}
			}
		}))

		require.NoError(t, src2.AppendEntries(func(yield func(KeyValuePair) bool) {
			for i := range 100 {
				if i%2 == 1 {
					continue
				}
				if !yield(KeyValuePair{
					Key:   []byte(fmt.Sprintf("%03d", i)),
					Value: []byte("src2"),
				}) {
					return
				}
			}

			for j := range 50 {
				i := 100 + j
				if !yield(KeyValuePair{
					Key:       []byte(fmt.Sprintf("%03d", i)),
					IsDeleted: true,
				}) {
					return
				}
			}
		}))

		err = dst.MergeTables(MergeTablesArgs{Srcs: []*SSTable{&src1, &src2}})
		assert.NoError(t, err)
		assert.Equal(t, uint64(150), dst.header.NumEntries)

		evenEntry, err, exists := util.Seq2At(dst.Entries(), 80)
		if assert.NoError(t, err) && assert.True(t, exists) {
			assert.Equal(t, []byte("080"), evenEntry.Key)
			assert.Equal(t, []byte("src2"), evenEntry.Value)
			assert.False(t, evenEntry.IsDeleted)
		}

		oddEntry, err, exists := util.Seq2At(dst.Entries(), 25)
		if assert.NoError(t, err) && assert.True(t, exists) {
			assert.Equal(t, []byte("025"), oddEntry.Key)
			assert.Equal(t, []byte("src1"), oddEntry.Value)
			assert.False(t, oddEntry.IsDeleted)
		}

		endEntry, err, exists := util.Seq2At(dst.Entries(), 125)
		if assert.NoError(t, err) && assert.True(t, exists) {
			assert.Equal(t, []byte("125"), endEntry.Key)
			assert.Empty(t, endEntry.Value)
			assert.True(t, endEntry.IsDeleted)
		}
	})

	t.Run("two tables small", func(t *testing.T) {
		dst, err := Open(OpenArgs{
			Path:    dir + "/two_tables_small.sst",
			Create:  true,
			Version: 5,
		})
		require.NoError(t, err)
		defer dst.Close()

		src1, err := Open(OpenArgs{
			Path:    dir + "/two_tables_small_1.sst",
			Create:  true,
			Version: 5,
		})
		require.NoError(t, err)
		defer src1.Close()

		src2, err := Open(OpenArgs{
			Path:    dir + "/two_tables_small_2.sst",
			Create:  true,
			Version: 3,
		})
		require.NoError(t, err)
		defer src2.Close()

		require.NoError(t, src1.AppendEntries(util.SeqOf(
			KeyValuePair{Key: []byte("all the rainbows"), Value: []byte("couldn't stop me")},
			KeyValuePair{Key: []byte("can you see"), Value: []byte("the ship is alive")},
			KeyValuePair{Key: []byte("can't you see"), Value: []byte("what I'm doing here")},
			KeyValuePair{Key: []byte("everybody's"), IsDeleted: true},
			KeyValuePair{Key: []byte("i know"), Value: []byte("what you're doing here")},
		)))

		require.NoError(t, src2.AppendEntries(util.SeqOf(
			KeyValuePair{Key: []byte("all the rainbows"), Value: []byte("could stop me")},
			KeyValuePair{Key: []byte("don't you know"), Value: []byte("what I'm doing here")},
			KeyValuePair{Key: []byte("everybody's"), Value: []byte("looking for someone")},
		)))

		err = dst.MergeTables(MergeTablesArgs{Srcs: []*SSTable{&src1, &src2}})
		assert.NoError(t, err)
		assert.Equal(t, uint64(6), dst.header.NumEntries)

		var allEntries []SSTableEntry
		for entry, err := range dst.Entries() {
			require.NoError(t, err)
			allEntries = append(allEntries, entry)
		}

		assert.Len(t, allEntries, 6)
		assert.Equal(t, "all the rainbows", string(allEntries[0].Key))
		assert.Equal(t, "could stop me", string(allEntries[0].Value))
		assert.Equal(t, "can you see", string(allEntries[1].Key))
		assert.Equal(t, "can't you see", string(allEntries[2].Key))
		assert.Equal(t, "don't you know", string(allEntries[3].Key))
		assert.Equal(t, "everybody's", string(allEntries[4].Key))
		assert.Equal(t, "looking for someone", string(allEntries[4].Value))
		assert.Equal(t, "i know", string(allEntries[5].Key))
	})
}
