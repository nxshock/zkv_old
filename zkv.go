package zkv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type coords struct {
	blockNum     int64
	recordOffset int64
}

// Db represents key/value storage.
type Db struct {
	f         *os.File
	buf       bytes.Buffer
	keys      map[int64]coords // ключ - координаты
	blockInfo map[int64]int64  // номер блока - смещение в файле

	currentBlockNum int64

	config Config

	mu sync.Mutex
}

// OpenWithConfig opens storage with specified config options.
func OpenWithConfig(path string, config *Config) (*Db, error) {
	var flag int

	if config != nil && config.ReadOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_CREATE | os.O_RDWR
	}

	return open(path, flag, config)
}

// Open opens storage with default config options.
func Open(path string) (*Db, error) {
	return open(path, os.O_CREATE|os.O_RDWR, nil)
}

func open(path string, fileFlags int, config *Config) (*Db, error) {
	newDb := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		newDb = true
	}

	if newDb && config != nil && config.ReadOnly {
		return nil, errors.New("trying to create new readonly storage")
	}

	f, err := os.OpenFile(path, fileFlags, 0644)
	if err != nil {
		return nil, err
	}

	db := &Db{
		f:         f,
		keys:      make(map[int64]coords),
		blockInfo: make(map[int64]int64)}

	if newDb {
		if config != nil && config.BlockDataSize > 0 {
			db.config.BlockDataSize = config.BlockDataSize
		} else {
			db.config.BlockDataSize = defaultConfig.BlockDataSize
		}

		err = writeHeader(db.f, db.config.BlockDataSize)
		if err != nil {
			db.f.Close()
			return nil, err
		}

		return db, nil
	}

	header, err := readHeader(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	db.config.BlockDataSize = header.blockDataSize

	if config != nil && config.ReadOnly {
		db.config.ReadOnly = config.ReadOnly
	}

	if config != nil && config.BlockDataSize > 0 && db.config.BlockDataSize != config.BlockDataSize {
		f.Close()
		return nil, fmt.Errorf("can't change block size to %d on existing storage with block size %d", config.BlockDataSize, db.config.BlockDataSize)
	}

	err = db.readAllBlocks()
	if err != nil {
		f.Close()
		return nil, err
	}

	err = db.move()
	if err != nil {
		f.Close()
		return nil, err
	}

	return db, nil
}

func (db *Db) readAllBlocks() error {
	_, err := db.f.Seek(headerLength, io.SeekStart)
	if err != nil {
		return err
	}

	for {
		blockStartPos, err := db.f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		blockData, err := readBlock(db.f)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		db.blockInfo[db.currentBlockNum] = blockStartPos

		blockDataReader := bytes.NewReader(blockData)
		for {
			recordOffset, err := blockDataReader.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}

			action, key, _, err := readRecord(blockDataReader)
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			switch action {
			case actionAdd:
				if _, exists := db.keys[key]; exists {
					return fmt.Errorf("unexpected add of key %d because it is already exists", key)
				}
				db.keys[key] = coords{blockNum: db.currentBlockNum, recordOffset: recordOffset}
			case actionDelete:
				if _, exists := db.keys[key]; !exists {
					return fmt.Errorf("unexpected delete of key %d because it is does not exists", key)
				}
				delete(db.keys, key)
			default:
				return fmt.Errorf("unknown action: %d for key %d", action, key)
			}
		}

		db.currentBlockNum++
	}
	return nil
}

// переместить последний блок в буфер
func (db *Db) move() error {
	if len(db.blockInfo) == 0 {
		return nil
	}

	offset, exists := db.blockInfo[db.currentBlockNum-1]
	if !exists {
		return fmt.Errorf("last block #%d is not present in db.blockInfo", db.currentBlockNum-1)
	}

	_, err := db.f.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	blockBytes, err := readBlock(db.f)
	if err != nil {
		return err
	}

	if int64(len(blockBytes)) >= db.config.BlockDataSize {
		return nil
	}

	db.buf.Reset()
	db.buf.Write(blockBytes)

	delete(db.blockInfo, db.currentBlockNum-1)
	db.currentBlockNum--

	return nil
}

// Set saves value for specified key.
// value can be any type.
func (db *Db) Set(key int64, value interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.config.ReadOnly {
		return errReadOnly
	}

	return db.set(key, value)
}

func (db *Db) set(key int64, value interface{}) error {
	c := coords{
		blockNum:     db.currentBlockNum,
		recordOffset: int64(db.buf.Len())}

	err := writeRecord(&db.buf, actionAdd, key, value)
	if err != nil {
		return err
	}
	db.keys[key] = c

	if int64(db.buf.Len()) >= db.config.BlockDataSize {
		err = db.flush()
		if err != nil {
			return err
		}
	}

	return nil
}

// Get returns value of specified key.
func (db *Db) Get(key int64, valuePtr interface{}) (exists bool, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.get(key, valuePtr)
}

func (db *Db) get(key int64, valuePtr interface{}) (exists bool, err error) {
	coords, exists := db.keys[key]
	if !exists {
		return false, nil
	}

	if coords.blockNum == db.currentBlockNum {
		r := bytes.NewReader(db.buf.Bytes())
		_, err := r.Seek(coords.recordOffset, io.SeekStart)
		if err != nil {
			return true, err
		}

		_, _, recordBytes, err := readRecord(r)
		if err != nil {
			return true, err
		}

		err = gob.NewDecoder(bytes.NewReader(recordBytes)).Decode(valuePtr)
		if err != nil {
			return true, err
		}

		return true, nil
	}

	_, err = db.f.Seek(db.blockInfo[coords.blockNum], io.SeekStart)
	if err != nil {
		return true, err
	}

	blockBytes, err := readBlock(db.f)
	if err != nil {
		return true, err
	}

	blockBytesReader := bytes.NewReader(blockBytes)
	blockBytesReader.Seek(coords.recordOffset, io.SeekStart)

	_, gotKey, valueBytes, err := readRecord(blockBytesReader)
	if err != nil {
		return true, err
	}

	if gotKey != key {
		return true, fmt.Errorf("expected read %d key, got %d", key, gotKey)
	}

	err = gob.NewDecoder(bytes.NewReader(valueBytes)).Decode(valuePtr)
	if err != nil {
		return true, err
	}

	return true, nil
}

func (db *Db) flush() error {
	if db.buf.Len() == 0 {
		return nil
	}

	blockOffset, err := db.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	err = writeBlock(db.f, db.buf.Bytes())
	if err != nil {
		return err
	}

	db.buf.Reset()
	db.blockInfo[db.currentBlockNum] = blockOffset
	db.currentBlockNum++

	return nil
}

// Close saves buffered data and closes storage.
func (db *Db) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.buf.Len() > 0 {
		err := db.flush()
		if err != nil {
			return err
		}
	}

	return db.f.Close()
}

// Keys returns all stored keys.
// Key order is not guaranteed.
func (db *Db) Keys() []int64 {
	db.mu.Lock()
	defer db.mu.Unlock()

	var keys []int64

	for key := range db.keys {
		keys = append(keys, key)
	}

	return keys
}

// Count returns number of stored key/value pairs.
func (db *Db) Count() int {
	db.mu.Lock()
	defer db.mu.Unlock()

	return len(db.keys)
}

// Delete deletes value of specified key.
func (db *Db) Delete(key int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.config.ReadOnly {
		return errReadOnly
	}

	return db.delete(key)
}

func (db *Db) delete(key int64) error {
	if _, exists := db.keys[key]; !exists {
		return nil
	}

	err := writeRecord(&db.buf, actionDelete, key, nil)
	if err != nil {
		return err
	}

	delete(db.keys, key)

	return nil
}
