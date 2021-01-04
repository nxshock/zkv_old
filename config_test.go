package zkv

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicConfig(t *testing.T) {
	const filePath = "file.tmp"
	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)

	assert.Equal(t, *defaultConfig, db.Config())

	err = db.Close()
	assert.NoError(t, err)
}

func TestCustomBlockSize(t *testing.T) {
	const filePath = "file.tmp"
	defer os.Remove(filePath)

	config := &Config{BlockDataSize: 4 * 1024 * 1024}

	db, err := OpenWithConfig(filePath, config)
	assert.NoError(t, err)
	assert.Equal(t, *config, db.config)

	err = db.Close()
	assert.NoError(t, err)
}