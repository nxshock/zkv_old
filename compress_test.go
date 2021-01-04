package zkv

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompress(t *testing.T) {
	for i := 0; i < 10; i++ {
		sourceBytes := []byte("source bytes" + strconv.Itoa(i))

		compressedBytes, err := compress(sourceBytes)
		assert.NoError(t, err)
		assert.NotNil(t, compressedBytes)

		decompressedBytes, err := decompress(compressedBytes)
		assert.NoError(t, err)
		assert.NotNil(t, decompressedBytes)

		assert.Equal(t, sourceBytes, decompressedBytes)
	}
}
