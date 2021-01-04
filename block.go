package zkv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
)

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
