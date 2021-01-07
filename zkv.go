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
	keys      map[string]coords // [key]block number + record offset
	blockInfo map[int64]int64   // [block number]file offset

	lastLoadedBlock struct {
		blockNum  int64
		blockData []byte
	}

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
		return nil, fmt.Errorf("open storage file: %v", err)
	}

	db := &Db{
		f:         f,
		keys:      make(map[string]coords),
		blockInfo: make(map[int64]int64)}
	db.lastLoadedBlock.blockNum = -1

	if newDb {
		if config != nil && config.BlockDataSize > 0 {
			db.config.BlockDataSize = config.BlockDataSize
		} else {
			db.config.BlockDataSize = defaultConfig.BlockDataSize
		}

		if config != nil && config.Compressor != nil {
			db.config.Compressor = config.Compressor
		} else {
			db.config.Compressor = defaultCompressor
		}

		err = writeHeader(db.f, db.config.BlockDataSize, db.config.Compressor.Id())
		if err != nil {
			db.f.Close()
			return nil, fmt.Errorf("write header: %v", err)
		}

		return db, nil
	}

	header, err := readHeader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %v", err)
	}

	compressor, exists := availableCompressors[header.compressorId]
	if !exists {
		f.Close()
		return nil, fmt.Errorf("unknown compressor id = %d", header.compressorId)
	}
	db.config.Compressor = compressor

	if config != nil && config.ReadOnly {
		db.config.ReadOnly = config.ReadOnly
	}

	db.config.BlockDataSize = header.blockDataSize
	if config != nil && config.BlockDataSize > 0 && db.config.BlockDataSize != config.BlockDataSize {
		f.Close()
		return nil, fmt.Errorf("can't change block size to %d on existing storage with block size %d", config.BlockDataSize, db.config.BlockDataSize)
	}

	if config != nil && config.Compressor != nil && db.config.Compressor.Id() != config.Compressor.Id() {
		f.Close()
		return nil, fmt.Errorf("can't change compressor to %d on existing storage with compressor %d", config.Compressor.Id(), db.config.Compressor.Id())
	}

	err = db.readAllBlocks()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("read stored records: %v", err)
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

		blockData, err := readBlock(db.f, db.config.Compressor)
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

			action, keyBytes, _, err := readRecord(blockDataReader)
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			switch action {
			case actionAdd:
				db.keys[string(keyBytes)] = coords{blockNum: db.currentBlockNum, recordOffset: recordOffset}
			case actionDelete:
				if _, exists := db.keys[string(keyBytes)]; !exists {
					return fmt.Errorf("unexpected delete of key %v because it is does not exists", keyBytes)
				}
				delete(db.keys, string(keyBytes))
			default:
				return fmt.Errorf("unknown action: %d for key %v", action, keyBytes)
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

	blockBytes, err := readBlock(db.f, db.config.Compressor)
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
// key and value can be any type.
func (db *Db) Set(key interface{}, value interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.config.ReadOnly {
		return errReadOnly
	}

	return db.set(key, value)
}

func (db *Db) set(key interface{}, value interface{}) error {
	keyBytes, err := encode(key)
	if err != nil {
		return err
	}

	valueBytes, err := encode(value)
	if err != nil {
		return err
	}

	return db.writeRecord(actionAdd, keyBytes, valueBytes)
}

// Get returns value of specified key.
func (db *Db) Get(key interface{}, valuePtr interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.get(key, valuePtr)
}

func (db *Db) get(key interface{}, valuePtr interface{}) error {
	keyBytes, err := encode(key)
	if err != nil {
		return err
	}

	action, gotKeyBytes, valueBytes, err := db.getRecord(keyBytes)
	if err != nil {
		return err
	}

	if action != actionAdd {
		return fmt.Errorf("expected %v action, got %v", actionAdd, action)
	}

	if !bytes.Equal(gotKeyBytes, keyBytes) {
		return fmt.Errorf("expected read %v key, got %v", keyBytes, gotKeyBytes)
	}

	err = gob.NewDecoder(bytes.NewReader(valueBytes)).Decode(valuePtr)
	if err != nil {
		return err
	}

	return nil
}

// Flush saves buffered data on disk.
func (db *Db) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.flush()
}

func (db *Db) flush() error {
	if db.buf.Len() == 0 {
		return nil
	}

	blockOffset, err := db.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	err = writeBlock(db.f, db.config.Compressor, db.buf.Bytes())
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
	keyBytes, err := encode(key)
	if err != nil {
		return err
	}

	if _, exists := db.keys[string(keyBytes)]; !exists {
		return nil
	}

	err = writeRecord(&db.buf, actionDelete, keyBytes, nil)
	if err != nil {
		return err
	}

	delete(db.keys, string(keyBytes))

	return nil
}

// Shrink compacts storage by removing replaced records and saves new file to
// specified path.
func (db *Db) Shrink(filePath string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	fileExists := func(filename string) bool {
		info, err := os.Stat(filename)
		if os.IsNotExist(err) {
			return false
		}
		return !info.IsDir()
	}

	if fileExists(filePath) {
		return fmt.Errorf("file %s must not exists", filePath)
	}

	shrinkedDb, err := OpenWithConfig(filePath, &Config{BlockDataSize: db.config.BlockDataSize})
	if err != nil {
		shrinkedDb.Close()
		os.Remove(filePath)
		return err
	}

	var keysBytes [][]byte
	for keyBytes := range db.keys {
		keysBytes = append(keysBytes, []byte(keyBytes))
	}

	for _, keysBytes := range keysBytes {
		action, gotKeyBytes, valueBytes, err := db.getRecord([]byte(keysBytes))
		if err != nil {
			return err
		}
		if !bytes.Equal(gotKeyBytes, []byte(keysBytes)) {
			return fmt.Errorf("expected %v key bytes, got %v", keysBytes, gotKeyBytes)
		}
		if action != actionAdd {
			return fmt.Errorf("expected %v action, got %v", actionAdd, action)
		}

		shrinkedDb.writeRecord(action, keysBytes, valueBytes)
	}

	return shrinkedDb.Close()
}

func (db *Db) writeRecord(action action, keyBytes []byte, valueBytes []byte) error {
	c := coords{
		blockNum:     db.currentBlockNum,
		recordOffset: int64(db.buf.Len())}

	err := writeRecord2(&db.buf, actionAdd, keyBytes, valueBytes)
	if err != nil {
		return err
	}
	db.keys[string(keyBytes)] = c

	if int64(db.buf.Len()) >= db.config.BlockDataSize {
		err = db.flush()
		if err != nil {
			return err
		}
	}

	return nil

}

func (db *Db) getRecord(keyBytes []byte) (action action, rKeyBytes []byte, valueBytes []byte, err error) {
	coords, exists := db.keys[string(keyBytes)]
	if !exists {
		return actionNone, nil, nil, errNotFound
	}

	switch {
	// load from write buffer
	case coords.blockNum == db.currentBlockNum:
		return readRecord(bytes.NewReader(db.buf.Bytes()[coords.recordOffset:]))
	// load from last decoded block
	case coords.blockNum == db.lastLoadedBlock.blockNum:
		return readRecord(bytes.NewReader(db.lastLoadedBlock.blockData[coords.recordOffset:]))
	}

	_, err = db.f.Seek(db.blockInfo[coords.blockNum], io.SeekStart)
	if err != nil {
		return actionNone, nil, nil, err
	}

	blockBytes, err := readBlock(db.f, db.config.Compressor)
	if err != nil {
		return actionNone, nil, nil, err
	}

	db.lastLoadedBlock.blockNum = coords.blockNum
	db.lastLoadedBlock.blockData = blockBytes

	blockBytesReader := bytes.NewReader(blockBytes)
	_, err = blockBytesReader.Seek(coords.recordOffset, io.SeekStart)
	if err != nil {
		return actionNone, nil, nil, err
	}

	return readRecord(blockBytesReader)
}
