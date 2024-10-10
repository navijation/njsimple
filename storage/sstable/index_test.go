package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_sparseMemIndex_LookupSearchLocation(t *testing.T) {
	t.Run("test empty", func(t *testing.T) {
		index := SparseMemIndex{}

		assert.Equal(t, EntryLocation{}, index.LookupSearchLocation([]byte("hello world")))
		assert.Equal(t, EntryLocation{}, index.LookupSearchLocation(nil))
	})

	t.Run("test empty", func(t *testing.T) {
		index := SparseMemIndex{
			IndexedEntries: []SparseMemIndexEntry{
				{
					Key: []byte("everybody knows\n"),
					Location: EntryLocation{
						EntryNumber: 5,
						Offset:      100,
					},
				},
				{
					Key: []byte("nobody knows"),
					Location: EntryLocation{
						EntryNumber: 6,
						Offset:      200,
					},
				},
				{
					Key: []byte("nobody knows2"),
					Location: EntryLocation{
						EntryNumber: 7,
						Offset:      250,
					},
				},
			},
		}

		assert.Equal(t, EntryLocation{}, index.LookupSearchLocation([]byte("everybody knows")))
		assert.Equal(t, EntryLocation{
			EntryNumber: 5,
			Offset:      100,
		}, index.LookupSearchLocation([]byte("everybody knows\n")))
		assert.Equal(t, EntryLocation{
			EntryNumber: 5,
			Offset:      100,
		}, index.LookupSearchLocation([]byte("everybody knows what I mean\n")))
		assert.Equal(t, EntryLocation{
			EntryNumber: 7,
			Offset:      250,
		}, index.LookupSearchLocation([]byte("xenophobe")))
	})
}
