package sstable

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ssTableHeader_serde(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header Header
	}{
		{
			name: "basic",
			header: Header{
				FileSize:   50,
				NumEntries: 3,
				Version:    1,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			n, err := tc.header.WriteTo(&buf)
			require.NoError(t, err)
			assert.Equal(t, tc.header.SizeOf(), uint64(n))

			var deser Header

			n, err = deser.ReadFrom(&buf)
			require.NoError(t, err)
			assert.Equal(t, tc.header.SizeOf(), uint64(n))

			assert.Equal(t, tc.header, deser)
		})
	}
}
