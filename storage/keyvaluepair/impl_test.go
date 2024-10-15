package keyvaluepair

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyValuePair_ToStoredKeyValuePair(t *testing.T) {
	for _, tc := range []struct {
		name string
		kvp  KeyValuePair

		expected StoredKeyValuePair
	}{
		{
			name: "not deleted",
			kvp: KeyValuePair{
				Key:       []byte("123456789101112"),
				Value:     []byte("abc"),
				IsDeleted: false,
			},

			expected: StoredKeyValuePair{
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

			expected: StoredKeyValuePair{
				keySizeAndTombstone: 15 | tombstoneMask,
				ValueSize:           3,
				Key:                 []byte("123456789101112"),
				Value:               []byte("abc"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.kvp.ToStoredKeyValuePair())
		})
	}
}

func Test_StoredKeyValuePair_serde(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stored StoredKeyValuePair

		expectedDeser StoredKeyValuePair
	}{
		{
			name: "not deleted",
			stored: StoredKeyValuePair{
				keySizeAndTombstone: 15,
				ValueSize:           3,
				Key:                 []byte("123456789101112"),
				Value:               []byte("abc"),
			},

			expectedDeser: StoredKeyValuePair{
				keySizeAndTombstone: 15,
				ValueSize:           3,
				Key:                 []byte("123456789101112"),
				Value:               []byte("abc"),
			},
		},
		{
			name: "deleted",
			stored: StoredKeyValuePair{
				keySizeAndTombstone: 15 | tombstoneMask,
				ValueSize:           3,
				Key:                 []byte("123456789101112"),
				Value:               []byte("abc"),
			},

			expectedDeser: StoredKeyValuePair{
				keySizeAndTombstone: 15 | tombstoneMask,
				Key:                 []byte("123456789101112"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			n, err := tc.stored.WriteTo(&buf)
			require.NoError(t, err)
			assert.Equal(t, uint64(n), tc.stored.SizeOf())

			var deser StoredKeyValuePair

			n, err = deser.ReadFrom(&buf)
			require.NoError(t, err)
			assert.Equal(t, uint64(n), tc.stored.SizeOf())

			assert.Equal(t, tc.expectedDeser, deser)
		})
	}
}
