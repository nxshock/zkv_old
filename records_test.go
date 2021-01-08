package zkv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyOperations(t *testing.T) {
	key := int64(123)
	bytes, err := Encode(key)
	assert.NoError(t, err)

	var gotKey int64
	err = Decode(bytes, &gotKey)
	assert.NoError(t, err)
	assert.Equal(t, key, gotKey)
}
