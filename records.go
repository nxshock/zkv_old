package zkv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
)

func encodeKey(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(key)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeKey(encodedKey []byte, keyPtr interface{}) error {
	return gob.NewDecoder(bytes.NewReader(encodedKey)).Decode(keyPtr)
}

func writeRecord2(w io.Writer, action action, keyBytes []byte, valueBytes []byte) error {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.LittleEndian, action)
	if err != nil {
		return err
	}

	err = binary.Write(&buf, binary.LittleEndian, int64(len(keyBytes)))
	if err != nil {
		return err
	}

	_, err = buf.Write(keyBytes)
	if err != nil {
		return err
	}

	switch action {
	case actionAdd:
		err = binary.Write(&buf, binary.LittleEndian, int64(len(valueBytes)))
		if err != nil {
			return err
		}

		buf.Write(valueBytes)
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

func writeRecord(w io.Writer, action action, keyBytes []byte, valuePtr interface{}) error {
	if valuePtr == nil {
		return writeRecord2(w, action, keyBytes, nil)
	}

	valueBytes, err := encodeKey(valuePtr)
	if err != nil {
		return err
	}

	return writeRecord2(w, action, keyBytes, valueBytes)
}

func readRecord(r io.Reader) (action action, keyBytes []byte, valueBytes []byte, err error) {
	err = binary.Read(r, binary.LittleEndian, &action)
	if err != nil {
		return actionNone, nil, nil, err
	}

	var keyBytesLength int64
	err = binary.Read(r, binary.LittleEndian, &keyBytesLength)
	if err != nil {
		return actionNone, nil, nil, err
	}

	if keyBytesLength <= 0 {
		return actionNone, nil, nil, fmt.Errorf("unexpected key bytes length = %d", keyBytesLength)
	}

	keyBytes = make([]byte, keyBytesLength)
	_, err = r.Read(keyBytes)
	if err != nil {
		return actionNone, nil, nil, err
	}

	switch action {
	case actionAdd:
		var recordDataLength int64
		err = binary.Read(r, binary.LittleEndian, &recordDataLength)
		if err != nil {
			return actionNone, nil, nil, err
		}

		if recordDataLength <= 0 {
			return actionNone, nil, nil, fmt.Errorf("unexpected record data length = %d", recordDataLength)
		}

		valueBytes = make([]byte, int(recordDataLength))
		_, err = r.Read(valueBytes)
		if err != nil {
			return actionNone, nil, nil, err
		}
		return action, keyBytes, valueBytes, nil
	case actionDelete:
		return action, keyBytes, nil, nil
	}

	return actionNone, nil, nil, fmt.Errorf("unknown action %d", action)
}

func (db *Db) getRecord(keyBytes []byte) (action action, rKeyBytes []byte, valueBytes []byte, err error) {
	coords, exists := db.keys[string(keyBytes)]
	if !exists {
		return actionNone, nil, nil, errNotFound
	}

	if coords.blockNum == db.currentBlockNum {
		r := bytes.NewReader(db.buf.Bytes())
		_, err := r.Seek(coords.recordOffset, io.SeekStart)
		if err != nil {
			return actionNone, nil, nil, err
		}

		return readRecord(r)
	}

	_, err = db.f.Seek(db.blockInfo[coords.blockNum], io.SeekStart)
	if err != nil {
		return actionNone, nil, nil, err
	}

	blockBytes, err := readBlock(db.f)
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