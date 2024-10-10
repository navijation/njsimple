package sstable

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyValuePair_ToInternalSSTableEntry(t *testing.T) {
	for _, tc := range []struct {
		name string
		kvp  KeyValuePair

		expected internalSSTableEntry
	}{
		{
			name: "not deleted",
			kvp: KeyValuePair{
				Key:       []byte("123456789101112"),
				Value:     []byte("abc"),
				IsDeleted: false,
			},

			expected: internalSSTableEntry{
				keySizeAndTombstone: 15,
				ValueSize:           3,
				Key:                 []byte("123456789101112"),
				Value:               []byte("abc"),
			},
		},
		{
			name: "deleted",
			kvp: KeyValuePair{
				Key:       []byte("123456789101112"),
				Value:     []byte("abc"),
				IsDeleted: true,
			},

			expected: internalSSTableEntry{
				keySizeAndTombstone: 15 | tombstoneMask,
				ValueSize:           3,
				Key:                 []byte("123456789101112"),
				Value:               []byte("abc"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.kvp.ToInternalSSTableEntry())
		})
	}
}

func Test_internalSSTableEntry_toSSTableEntry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		location EntryLocation
		internal internalSSTableEntry

		expect SSTableEntry
	}{
		{
			name: "not deleted",
			internal: internalSSTableEntry{
				keySizeAndTombstone: 15,
				ValueSize:           3,
				Key:                 []byte("123456789101112"),
				Value:               []byte("abc"),
			},
			location: EntryLocation{
				EntryNumber: 2,
				Offset:      54,
			},

			expect: SSTableEntry{
				Location: EntryLocation{
					EntryNumber: 2,
					Offset:      54,
				},
				KeySize:   15,
				ValueSize: 3,
				Key:       []byte("123456789101112"),
				Value:     []byte("abc"),
				IsDeleted: false,
			},
		},
		{
			name: "deleted",
			internal: internalSSTableEntry{
				keySizeAndTombstone: 15 | tombstoneMask,
				ValueSize:           3,
				Key:                 []byte("123456789101112"),
				Value:               []byte("abc"),
			},
			location: EntryLocation{
				EntryNumber: 2,
				Offset:      54,
			},

			expect: SSTableEntry{
				Location: EntryLocation{
					EntryNumber: 2,
					Offset:      54,
				},
				KeySize:   15,
				ValueSize: 3,
				Key:       []byte("123456789101112"),
				Value:     []byte("abc"),
				IsDeleted: true,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, tc.internal.ToSSTableEntry(tc.location))
		})
	}
}

func Test_internalSSTableEntry_serde(t *testing.T) {
	for _, tc := range []struct {
		name     string
		internal internalSSTableEntry
	}{
		{
			name: "not deleted",
			internal: internalSSTableEntry{
				keySizeAndTombstone: 15,
				ValueSize:           3,
				Key:                 []byte("123456789101112"),
				Value:               []byte("abc"),
			},
		},
		{
			name: "deleted",
			internal: internalSSTableEntry{
				keySizeAndTombstone: 15 | tombstoneMask,
				ValueSize:           3,
				Key:                 []byte("123456789101112"),
				Value:               []byte("abc"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			n, err := tc.internal.WriteTo(&buf)
			require.NoError(t, err)
			assert.Equal(t, uint64(n), tc.internal.SizeOf())

			var deser internalSSTableEntry

			n, err = deser.ReadFrom(&buf)
			require.NoError(t, err)
			assert.Equal(t, uint64(n), tc.internal.SizeOf())

			assert.Equal(t, deser, tc.internal)
		})
	}
}
