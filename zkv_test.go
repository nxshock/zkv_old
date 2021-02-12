package zkv

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitFile(t *testing.T) {
	const filePath = "init.tmp"
	defer os.Remove(filePath)

	err := initDb(filePath, *defaultConfig)
	assert.NoError(t, err)
	assert.FileExists(t, filePath)

	stat, err := os.Stat(filePath)
	assert.NoError(t, err)
	assert.EqualValues(t, 13, stat.Size())
}

func TestFlush(t *testing.T) {
	const filePath = "flush.tmp"
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
	const filePath = "empty.tmp"

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
	const filePath = "basic.tmp"

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
	const filePath = "read.tmp"
	const expectedRecordCount = 10

	defer os.Remove(filePath)

	config := &Config{BlockDataSize: 1 * 1024}

	db, err := OpenWithConfig(filePath, config)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.EqualValues(t, 0, db.currentBlockNum)
	assert.EqualValues(t, map[int64]int64{}, db.blockInfo)

	for i := int64(0); i < expectedRecordCount; i++ {
		err = db.Set(i, i)
		assert.NoError(t, err)
	}
	assert.Equal(t, expectedRecordCount, db.Count())

	for i := int64(0); i < expectedRecordCount; i++ {
		var got int64

		err = db.Get(i, &got)
		assert.NoError(t, err)
		assert.EqualValues(t, i, got)
	}

	blockOnDisk := len(db.blockInfo)
	blockInMemBytes := db.buf.Bytes()
	bytesInMem := db.buf.Len()
	currentBlockNum := db.currentBlockNum
	storedKeys := len(db.keys)

	err = db.Close()
	assert.NoError(t, err)

	db, err = Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	assert.EqualValues(t, db.currentBlockNum, currentBlockNum)
	assert.Len(t, db.blockInfo, blockOnDisk)
	assert.EqualValues(t, bytesInMem, db.buf.Len())
	assert.Len(t, db.keys, storedKeys)
	assert.Equal(t, blockInMemBytes, db.buf.Bytes())

	for i := int64(0); i < expectedRecordCount; i++ {
		var got int64

		err = db.Get(i, &got)
		assert.NoError(t, err)
		assert.EqualValues(t, i, got)
	}

	err = db.Close()
	assert.NoError(t, err)
}

func TestOneWriteRead(t *testing.T) {
	const filePath = "one.tmp"

	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.Len(t, db.blockInfo, 0)

	err = db.Set(1, 1)
	assert.NoError(t, err)
	//assert.Equal(t, []int64{1}, db.Keys())

	var got int64
	err = db.Get(1, &got)
	assert.NoError(t, err)

	// -------------------------------------------------------------------------

	err = db.flush()
	assert.NoError(t, err)
	assert.Len(t, db.blockInfo, 1)

	//assert.Equal(t, []int64{1}, db.Keys())
	got = 0
	err = db.Get(1, &got)
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	// -------------------------------------------------------------------------

	db, err = Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	//assert.Equal(t, []int64{1}, db.Keys())
	got = 0
	err = db.Get(1, &got)
	assert.NoError(t, err)

	assert.EqualValues(t, 1, got)

	err = db.Close()
	assert.NoError(t, err)
}

func TestDelete(t *testing.T) {
	const filePath = "delete.tmp"

	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	err = db.Set(1, 1)
	assert.NoError(t, err)

	err = db.Delete(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, db.Count())

	err = db.Close()
	assert.NoError(t, err)

	db, err = Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.Equal(t, 0, db.Count())

	err = db.Get(1, nil)
	assert.Equal(t, ErrNotFound, err)

	err = db.Close()
	assert.NoError(t, err)
}

func TestKeyReplacing(t *testing.T) {
	const filePath = "replace.tmp"

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

func TestShrink(t *testing.T) {
	const filePath = "shrink1.zkv"
	const newFilePath = "shrink2.zkv"

	defer os.Remove(filePath)
	defer os.Remove(newFilePath)

	db, err := Open(filePath)
	assert.NoError(t, err)

	for i := 0; i < 10000; i++ {
		err = db.Set(i/10, i)
		assert.NoError(t, err)
	}

	err = db.Close()
	assert.NoError(t, err)

	db, err = Open(filePath)
	assert.NoError(t, err)

	err = db.Shrink(newFilePath)
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	file1stat, err := os.Stat(filePath)
	assert.NoError(t, err)
	file2stat, err := os.Stat(newFilePath)
	assert.NoError(t, err)

	assert.True(t, file1stat.Size() > file2stat.Size())
}

func TestIterateKeys(t *testing.T) {
	const filePath = "iterate.tmp"

	defer os.Remove(filePath)

	db, err := Open(filePath)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	for i := 1; i < 100; i++ {
		err = db.Set(i, i)
		assert.NoError(t, err)
	}
	for i := 1; i < 100; i++ {
		err = db.Delete(i)
		assert.NoError(t, err)
	}

	var expectedKeyOrder []int
	for i := 100; i < 200; i++ {
		err = db.Set(i, i)
		assert.NoError(t, err)

		expectedKeyOrder = append(expectedKeyOrder, i)
	}

	var gotKeyOrder []int
	db.Iterate(func(k, v []byte) bool {
		var kv int
		err = Decode(k, &kv)
		assert.NoError(t, err)

		gotKeyOrder = append(gotKeyOrder, kv)

		return true
	})

	assert.Equal(t, expectedKeyOrder, gotKeyOrder)

	err = db.Close()
	assert.NoError(t, err)
}
