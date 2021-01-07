package zkv

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadWriteRecord(t *testing.T) {
	var buf bytes.Buffer

	var record = struct {
		action action
		key    int64
		value  int64
	}{actionAdd, 123, 456}

	keyBytes, err := encode(record.key)
	assert.NoError(t, err)

	err = writeRecord(&buf, record.action, keyBytes, record.value)
	assert.NoError(t, err)

	action, keyBytes, b, err := readRecord(&buf)
	assert.NoError(t, err)

	var gotValue int64
	err = gob.NewDecoder(bytes.NewReader(b)).Decode(&gotValue)
	assert.NoError(t, err)

	assert.Equal(t, record.action, action)

	var key int64
	err = decodeKey(keyBytes, &key)
	assert.NoError(t, err)

	assert.Equal(t, record.key, key)
	assert.Equal(t, record.value, gotValue)
}

func TestReadWriteBlock(t *testing.T) {
	var buf bytes.Buffer
	var recordBuf bytes.Buffer

	for i := int64(0); i < 100; i++ {
		recordBuf.Reset()

		keyBytes, err := encode(i)
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
