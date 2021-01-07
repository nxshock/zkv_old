package zkv

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressors(t *testing.T) {
	for _, compressor := range availableCompressors {
		for i := 0; i < 10; i++ {
			sourceBytes := []byte("source bytes" + strconv.Itoa(i))

			compressedBytes, err := compressor.Compress(sourceBytes)
			assert.NoError(t, err)
			assert.NotNil(t, compressedBytes, "compressor", compressor.Id())

			decompressedBytes, err := compressor.Decompress(compressedBytes)
			assert.NoError(t, err)
			assert.NotNil(t, decompressedBytes, "compressor", compressor.Id())

			assert.Equal(t, sourceBytes, decompressedBytes)
		}
	}
}
