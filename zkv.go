package zkv

import (
	"bytes"
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
	filePath  string
	buf       bytes.Buffer
	keys      map[string]coords // [key]block number + record offset
	blockInfo map[int64]int64   // [block number]file offset

	currentBlockNum int64

	config Config

	mu sync.RWMutex
}

// OpenWithConfig opens storage with specified config options.
func OpenWithConfig(path string, config *Config) (*Db, error) {
	return open(path, config)
}

// Open opens storage with default config options.
func Open(path string) (*Db, error) {
	return open(path, nil)
}

func open(path string, config *Config) (*Db, error) {
	newDb := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		newDb = true
		config = defaultConfig
	}

	if newDb && config.ReadOnly {
		return nil, errors.New("trying to create new readonly storage")
	}

	var f *os.File
	var err error

	if newDb {
		err = initDb(path, *config)
		if err != nil {
			return nil, fmt.Errorf("init file: %v", err)
		}
	}

	f, err = os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open storage file: %v", err)
	}
	defer f.Close()

	db := &Db{
		filePath:  path,
		keys:      make(map[string]coords),
		blockInfo: make(map[int64]int64)}

	header, err := readHeader(f)
	if err != nil {
		return nil, fmt.Errorf("read header: %v", err)
	}

	compressor, exists := availableCompressors[header.compressorId]
	if !exists {
		return nil, fmt.Errorf("unknown compressor id = %d", header.compressorId)
	}
	db.config.Compressor = compressor

	if config != nil && config.ReadOnly {
		db.config.ReadOnly = config.ReadOnly
	}

	db.config.BlockDataSize = header.blockDataSize
	if config != nil && config.BlockDataSize > 0 && db.config.BlockDataSize != config.BlockDataSize {
		return nil, fmt.Errorf("can't change block size to %d on existing storage with block size %d", config.BlockDataSize, db.config.BlockDataSize)
	}

	if config != nil && config.Compressor != nil && db.config.Compressor.Id() != config.Compressor.Id() {
		return nil, fmt.Errorf("can't change compressor to %d on existing storage with compressor %d", config.Compressor.Id(), db.config.Compressor.Id())
	}

	err = db.readAllBlocks()
	if err != nil {
		return nil, fmt.Errorf("read stored records: %v", err)
	}

	err = db.restoreWriteBuffer()
	if err != nil {
		return nil, fmt.Errorf("restoreWriteBuffer: %v", err)
	}

	return db, nil
}

func initDb(filePath string, config Config) error {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("create file: %v", err)
	}

	err = writeHeader(f, config.BlockDataSize, config.Compressor.Id())
	if err != nil {
		return fmt.Errorf("write file header: %v", err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("close file: %v", err)
	}

	return nil
}

func (db *Db) readAllBlocks() error {
	f, err := os.Open(db.filePath)
	if err != nil {
		return fmt.Errorf("open file: %v", err)
	}
	defer f.Close()

	_, err = f.Seek(headerLength, io.SeekStart)
	if err != nil {
		return fmt.Errorf("file seek: %v", err)
	}

	for {
		blockStartPos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		blockData, err := readBlock(f, db.config.Compressor)
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

// restore write buffer
func (db *Db) restoreWriteBuffer() error {
	if len(db.blockInfo) == 0 {
		return nil
	}

	offset, exists := db.blockInfo[db.currentBlockNum-1]
	if !exists {
		return fmt.Errorf("last block #%d is not present in db.blockInfo", db.currentBlockNum-1)
	}

	f, err := os.Open(db.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	blockBytes, err := readBlock(f, db.config.Compressor)
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
	keyBytes, err := Encode(key)
	if err != nil {
		return err
	}

	valueBytes, err := Encode(value)
	if err != nil {
		return err
	}

	return db.writeRecord(actionAdd, keyBytes, valueBytes)
}

// Get returns value of specified key.
func (db *Db) Get(key interface{}, valuePtr interface{}) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.get(key, valuePtr)
}

func (db *Db) get(key interface{}, valuePtr interface{}) error {
	keyBytes, err := Encode(key)
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

	err = Decode(valueBytes, valuePtr)
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

	f, err := os.OpenFile(db.filePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	blockOffset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	err = writeBlock(f, db.config.Compressor, db.buf.Bytes())
	if err != nil {
		return err
	}

	db.buf.Reset()
	db.blockInfo[db.currentBlockNum] = blockOffset
	db.currentBlockNum++

	return f.Close()
}

// Close saves buffered data and closes storage.
func (db *Db) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.flush()
}

// Count returns number of stored key/value pairs.
func (db *Db) Count() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return len(db.keys)
}

// Delete deletes value of specified key.
func (db *Db) Delete(key interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.config.ReadOnly {
		return errReadOnly
	}

	return db.delete(key)
}

func (db *Db) delete(key interface{}) error {
	keyBytes, err := Encode(key)
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
	db.mu.RLock()
	defer db.mu.RUnlock()

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
		return actionNone, nil, nil, ErrNotFound
	}

	blockBytes, err := db.getBlockBytes(coords.blockNum)
	if err != nil {
		return actionNone, nil, nil, err
	}

	blockBytesReader := bytes.NewReader(blockBytes)
	_, err = blockBytesReader.Seek(coords.recordOffset, io.SeekStart)
	if err != nil {
		return actionNone, nil, nil, err
	}

	return readRecord(blockBytesReader)
}

func (db *Db) getBlockBytes(blockNum int64) ([]byte, error) {
	// load from write buffer
	if blockNum == db.currentBlockNum {
		return db.buf.Bytes(), nil
	}

	offset, exists := db.blockInfo[blockNum]
	if !exists {
		return nil, fmt.Errorf("block #%d does not exits", blockNum)
	}

	b, err := db.getBlockBytesFromFile(offset)
	if err != nil {
		return nil, fmt.Errorf("getBlockBytesFromFile: %v", err)
	}

	return b, nil
}

func (db *Db) getBlockBytesFromFile(offset int64) ([]byte, error) {
	f, err := os.Open(db.filePath)
	if err != nil {
		return nil, fmt.Errorf("open file: %v", err)
	}
	defer f.Close()

	_, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("file seek: %v", err)
	}

	return readBlock(f, db.config.Compressor)
}

// Iterate provedes fastest possible method of all record iteration.
func (db *Db) Iterate(f func(gobKeyBytes, gobValueBytes []byte) (continueIteration bool)) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for i := int64(0); i <= db.currentBlockNum; i++ {
		blockBytes, err := db.getBlockBytes(i)
		if err != nil {
			return err
		}

		blockDataReader := bytes.NewReader(blockBytes)
		for {
			recordOffset, err := blockDataReader.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}

			action, keyBytes, valueBytes, err := readRecord(blockDataReader)
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			if _, exists := db.keys[string(keyBytes)]; !exists {
				continue
			}

			if action != actionAdd ||
				db.keys[string(keyBytes)].blockNum != i ||
				db.keys[string(keyBytes)].recordOffset != recordOffset {
				continue
			}

			if !f(keyBytes, valueBytes) {
				return nil
			}
		}
	}

	return nil
}
