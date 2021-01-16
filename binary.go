package zkv

import "github.com/kelindar/binary"

// Encode transmits the data item to encoded []byte
func Encode(value interface{}) ([]byte, error) {
	return binary.Marshal(value)
}

// Decode reads value from encoded []byte
func Decode(valueBytes []byte, valuePtr interface{}) error {
	return binary.Unmarshal(valueBytes, valuePtr)
}
