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

	keyBytes, err := Encode(record.key)
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
	err = Decode(keyBytes, &key)
	assert.NoError(t, err)

	assert.Equal(t, record.key, key)
	assert.Equal(t, record.value, gotValue)
}

func TestKeyOperations(t *testing.T) {
	key := int64(123)
	bytes, err := Encode(key)
	assert.NoError(t, err)

	var gotKey int64
	err = Decode(bytes, &gotKey)
	assert.NoError(t, err)
	assert.Equal(t, key, gotKey)
}
