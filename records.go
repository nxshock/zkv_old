package zkv

import (
	"bytes"
	"fmt"
	"io"

	"github.com/kelindar/binary"
)

func writeRecord2(w io.Writer, action action, keyBytes []byte, valueBytes []byte) error {
	buf := new(bytes.Buffer)

	enc := binary.NewEncoder(buf)
	err := enc.Encode(action)
	if err != nil {
		return err
	}

	err = enc.Encode(keyBytes)
	if err != nil {
		return err
	}

	switch action {
	case actionAdd:
		err = enc.Encode(valueBytes)
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

func readRecord(r Reader) (action action, keyBytes []byte, valueBytes []byte, err error) {
	dec := binary.NewDecoder(r)

	err = dec.Decode(&action)
	if err != nil {
		return actionNone, nil, nil, err
	}

	err = dec.Decode(&keyBytes)
	if err != nil {
		return actionNone, nil, nil, err
	}

	switch action {
	case actionAdd:
		err = dec.Decode(&valueBytes)
		if err != nil {
			return actionNone, nil, nil, err
		}

		return action, keyBytes, valueBytes, nil
	case actionDelete:
		return action, keyBytes, nil, nil
	}

	return actionNone, nil, nil, fmt.Errorf("unknown action %d", action)
}
