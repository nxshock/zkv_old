package zkv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"
)

type coords struct {
	blockNum     int64
	recordOffset int64
}

type Db struct {
	f         *os.File
	buf       bytes.Buffer
	keys      map[int64]coords // ключ - координаты
	blockInfo map[int64]int64  // номер блока - смещение в файле

	currentBlockNum int64

	blockDataSize int64

	mu sync.Mutex
}

func Open(path string) (*Db, error) {
	return open(path, os.O_CREATE|os.O_RDWR)
}

func open(path string, fileFlags int) (*Db, error) {
	newDb := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		newDb = true
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
		db.blockDataSize = defaultBlockDataSize

		err = writeHeader(db.f, db.blockDataSize)
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

	db.blockDataSize = header.blockDataSize

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

	if int64(len(blockBytes)) >= db.blockDataSize {
		return nil
	}

	db.buf.Reset()
	db.buf.Write(blockBytes)

	delete(db.blockInfo, db.currentBlockNum-1)
	db.currentBlockNum--

	return nil
}

func (db *Db) Set(key int64, value interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

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

	if int64(db.buf.Len()) >= db.blockDataSize {
		err = db.flush()
		if err != nil {
			return err
		}
	}

	return nil
}

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

func writeRecord(w io.Writer, action action, key int64, value interface{}) error {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.LittleEndian, action)
	if err != nil {
		return err
	}

	err = binary.Write(&buf, binary.LittleEndian, key)
	if err != nil {
		return err
	}

	switch action {
	case actionAdd:
		var encodedDataBuf bytes.Buffer

		err = gob.NewEncoder(&encodedDataBuf).Encode(value)
		if err != nil {
			return err
		}

		err = binary.Write(&buf, binary.LittleEndian, int64(encodedDataBuf.Len()))
		if err != nil {
			return err
		}

		_, err = encodedDataBuf.WriteTo(&buf)
		if err != nil {
			return err
		}
	case actionDelete:
		// no additional fields
	default:
		return fmt.Errorf("can't write unknown action %v", action)
	}

	_, err = buf.WriteTo(w)
	if err != nil {
		return err
	}

	return nil
}

func writeBlock(w io.Writer, data []byte) error {
	compressedBlockData, err := compress(data)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, int64(len(compressedBlockData)))
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, int64(len(data)))
	if err != nil {
		return err
	}

	_, err = w.Write(compressedBlockData)
	if err != nil {
		return err
	}

	return nil
}

func readBlock(r io.Reader) (decompressedData []byte, err error) {
	var blockLength int64
	err = binary.Read(r, binary.LittleEndian, &blockLength)
	if err != nil {
		return nil, err
	}

	var dataLength int64
	err = binary.Read(r, binary.LittleEndian, &dataLength)
	if err != nil {
		return nil, err
	}

	if dataLength < 0 {
		return nil, fmt.Errorf("unexpected data length: %d", dataLength)
	}

	b := make([]byte, int(blockLength))

	_, err = r.Read(b)
	if err != nil {
		return nil, err
	}

	dataBytes, err := decompress(b)
	if err != nil {
		return nil, err
	}

	return dataBytes, nil
}

func readRecord(r io.Reader) (action action, key int64, valueGobBytes []byte, err error) {
	err = binary.Read(r, binary.LittleEndian, &action)
	if err != nil {
		return actionNone, 0, nil, err
	}

	err = binary.Read(r, binary.LittleEndian, &key)
	if err != nil {
		return actionNone, 0, nil, err
	}

	switch action {
	case actionAdd:
		var recordDataLength int64
		err = binary.Read(r, binary.LittleEndian, &recordDataLength)
		if err != nil {
			return actionNone, 0, nil, err
		}

		if recordDataLength <= 0 {
			return actionNone, 0, nil, fmt.Errorf("unexpected record data length = %d", recordDataLength)
		}

		valueGobBytes = make([]byte, int(recordDataLength))
		_, err = r.Read(valueGobBytes)
		if err != nil {
			return actionNone, 0, nil, err
		}
		return action, key, valueGobBytes, nil
	case actionDelete:
		return action, key, nil, nil
	}

	return actionNone, 0, nil, fmt.Errorf("unknown action %d", action)
}

func (db *Db) Keys() []int64 {
	db.mu.Lock()
	defer db.mu.Unlock()

	var keys []int64

	for key := range db.keys {
		keys = append(keys, key)
	}

	return keys
}

func (db *Db) Count() int {
	db.mu.Lock()
	defer db.mu.Unlock()

	return len(db.keys)
}

func (db *Db) Delete(key int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

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