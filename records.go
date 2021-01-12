package zkv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
)

// Encode transmits the data item to encoded []byte
func Encode(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(key)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode reads value from encoded []byte
func Decode(encodedKey []byte, keyPtr interface{}) error {
	return gob.NewDecoder(bytes.NewReader(encodedKey)).Decode(keyPtr)
}

func writeRecord2(w io.Writer, action action, keyBytes []byte, valueBytes []byte) error {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.LittleEndian, action)
	if err != nil {
		return err
	}

	_, err = buf.Write(keyBytes)
	if err != nil {
		return err
	}

	switch action {
	case actionAdd:
		_, err = buf.Write(valueBytes)
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

func writeRecord(w io.Writer, action action, keyBytes []byte, valuePtr interface{}) error {
	if valuePtr == nil {
		return writeRecord2(w, action, keyBytes, nil)
	}

	valueBytes, err := Encode(valuePtr)
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

	keyBytes, err = readGobData(r)
	if err != nil {
		return actionNone, nil, nil, err
	}

	switch action {
	case actionAdd:
		valueBytes, err = readGobData(r)
		if err != nil {
			return actionNone, nil, nil, err
		}
		return action, keyBytes, valueBytes, nil
	case actionDelete:
		return action, keyBytes, nil, nil
	}

	return actionNone, nil, nil, fmt.Errorf("unknown action %d", action)
}
