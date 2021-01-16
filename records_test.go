package zkv

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadWriteRecord(t *testing.T) {
	var buf bytes.Buffer

	type T struct {
		Value int64
	}

	var record = struct {
		action action
		key    int64
		value  T
	}{actionAdd, 123, T{456}}

	keyBytes, err := Encode(record.key)
	assert.NoError(t, err)

	err = writeRecord(&buf, record.action, keyBytes, record.value)
	assert.NoError(t, err)

	action, keyBytes, b, err := readRecord(&buf)
	assert.NoError(t, err)

	var gotValue T
	err = Decode(b, &gotValue)
	assert.NoError(t, err)

	assert.Equal(t, record.action, action)

	var key int64
	err = Decode(keyBytes, &key)
	assert.NoError(t, err)

	assert.Equal(t, record.key, key)
	assert.Equal(t, record.value, gotValue)
}
