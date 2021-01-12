package zkv

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGob1(t *testing.T) {
	for i := 1; i < 100000; i *= 10 {
		buf := new(bytes.Buffer)
		err := gob.NewEncoder(buf).Encode(make([]byte, i))
		assert.NoError(t, err)
		expectedLength := buf.Len()

		l, b, err := getGobDataLength(buf)
		assert.NoError(t, err)
		assert.Equal(t, expectedLength, l+len(b), "i =", i)
	}
}

func TestGob2(t *testing.T) {
	for i := 1; i < 10000; i *= 10 {
		buf := new(bytes.Buffer)
		err := gob.NewEncoder(buf).Encode(make([]byte, i))
		assert.NoError(t, err)
		expectedLength := buf.Len()

		b, err := readGobData(buf)
		assert.NoError(t, err)
		assert.Len(t, b, expectedLength)

		b2, err := readGobData(bytes.NewReader(b))
		assert.NoError(t, err)
		assert.Len(t, b2, expectedLength)

		_, err = readGobData(buf)
		assert.Error(t, err)
	}
}

func TestGob3(t *testing.T) {
	buf := new(bytes.Buffer)

	ss := []string{"123", "456", "789"}

	for i := 0; i < len(ss); i++ {
		err := gob.NewEncoder(buf).Encode(ss[i])
		assert.NoError(t, err)
	}

	for i := 0; i < len(ss); i++ {
		b, err := readGobData(buf)
		assert.NoError(t, err)

		var s string
		err = gob.NewDecoder(bytes.NewReader(b)).Decode(&s)
		assert.NoError(t, err)
		assert.Equal(t, ss[i], s)
	}
}
