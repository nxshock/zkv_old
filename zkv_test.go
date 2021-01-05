package zkv

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlush(t *testing.T) {
	const filePath = "file.tmp"
	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	assert.EqualValues(t, 0, db.currentBlockNum)
	assert.Empty(t, db.blockInfo)

	err = db.Set(1, 1)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, db.currentBlockNum)
	assert.True(t, db.buf.Len() > 0)

	err = db.flush()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, db.currentBlockNum)
	assert.Len(t, db.blockInfo, 1)
	assert.EqualValues(t, 0, db.buf.Len())

	err = db.Close()
	assert.NoError(t, err)
}

func TestEmpty(t *testing.T) {
	const filePath = "file.tmp"

	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	err = db.Close()
	assert.NoError(t, err)

	db, err = Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	err = db.Close()
	assert.NoError(t, err)
}

func TestBasic(t *testing.T) {
	const filePath = "file.tmp"

	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.EqualValues(t, 0, db.currentBlockNum)
	assert.EqualValues(t, map[int64]int64{}, db.blockInfo)

	for i := int64(0); i < 10000; i++ {
		err = db.Set(i, i)
		assert.NoError(t, err)
	}

	err = db.Close()
	assert.NoError(t, err)
}

func TestReadFile(t *testing.T) {
	const filePath = "file.tmp"

	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.EqualValues(t, 0, db.currentBlockNum)
	assert.EqualValues(t, map[int64]int64{}, db.blockInfo)

	for i := int64(0); i < 10000; i++ {
		err = db.Set(i, i)
		assert.NoError(t, err)
	}
	assert.Equal(t, 10000, db.Count())

	for i := int64(0); i < 10000; i++ {
		var got int64

		exists, err := db.Get(i, &got)
		assert.NoError(t, err)
		assert.True(t, exists)
		assert.EqualValues(t, i, got)
	}

	blockOnDisk := len(db.blockInfo)
	bytesInMem := db.buf.Len()
	currentBlockNum := db.currentBlockNum

	err = db.Close()
	assert.NoError(t, err)

	db, err = Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	assert.EqualValues(t, db.currentBlockNum, currentBlockNum)
	assert.Len(t, db.blockInfo, blockOnDisk)
	assert.EqualValues(t, bytesInMem, db.buf.Len())

	for i := int64(0); i < 10000; i++ {
		var got int64

		exists, err := db.Get(i, &got)
		assert.NoError(t, err)
		assert.True(t, exists)
		assert.EqualValues(t, i, got)
	}

	err = db.Close()
	assert.NoError(t, err)
}

func TestOneWriteRead(t *testing.T) {
	const filePath = "file.zkv"

	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.Len(t, db.blockInfo, 0)

	err = db.Set(1, 1)
	assert.NoError(t, err)
	assert.Equal(t, []int64{1}, db.Keys())

	var got int64
	exists, err := db.Get(1, &got)
	assert.NoError(t, err)
	assert.True(t, exists)

	// -------------------------------------------------------------------------

	err = db.flush()
	assert.NoError(t, err)
	assert.Len(t, db.blockInfo, 1)

	assert.Equal(t, []int64{1}, db.Keys())
	got = 0
	exists, err = db.Get(1, &got)
	assert.NoError(t, err)
	assert.True(t, exists)

	err = db.Close()
	assert.NoError(t, err)

	// -------------------------------------------------------------------------

	db, err = Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	assert.Equal(t, []int64{1}, db.Keys())
	got = 0
	exists, err = db.Get(1, &got)
	assert.NoError(t, err)
	assert.True(t, exists)

	assert.EqualValues(t, 1, got)

	err = db.Close()
	assert.NoError(t, err)
}

func TestDelete(t *testing.T) {
	const filePath = "file.zkv"

	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	err = db.Set(1, 1)
	assert.NoError(t, err)

	err = db.Delete(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, db.Count())
	assert.Empty(t, db.Keys())

	err = db.Close()
	assert.NoError(t, err)

	db, err = Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.Equal(t, 0, db.Count())
	assert.Empty(t, db.Keys())

	exists, err := db.Get(1, nil)
	assert.NoError(t, err)
	assert.False(t, exists)

	err = db.Close()
	assert.NoError(t, err)
}

func TestKeyReplacing(t *testing.T) {
	const filePath = "file.zkv"

	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	err = db.Set(1, 1)
	assert.NoError(t, err)

	err = db.Set(1, 1)
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	db, err = Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	err = db.Close()
	assert.NoError(t, err)
}
