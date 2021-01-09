package zkv

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadWriteBlock(t *testing.T) {
	var buf bytes.Buffer
	var recordBuf bytes.Buffer

	for i := int64(0); i < 100; i++ {
		recordBuf.Reset()

		keyBytes, err := Encode(i)
		assert.NoError(t, err)

		err = writeRecord(&recordBuf, actionAdd, keyBytes, i)
		assert.NoError(t, err)
	}

	err := writeBlock(&buf, XzCompressor, recordBuf.Bytes())
	assert.NoError(t, err)

	b, err := readBlock(&buf, XzCompressor)
	assert.NoError(t, err)

	assert.Equal(t, recordBuf.Bytes(), b)
}
