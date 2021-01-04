package zkv

import (
	"bytes"

	"github.com/klauspost/compress/zstd"
)

var encoder *zstd.Encoder
var buf bytes.Buffer

func init() {
	var err error

	encoder, err = zstd.NewWriter(nil)
	if err != nil {
		panic(err)
	}
}

func compress(b []byte) ([]byte, error) {
	defer buf.Reset()

	encoder.Reset(&buf)

	_, err := encoder.Write(b)
	if err != nil {
		return nil, err
	}

	err = encoder.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decompress(b []byte) ([]byte, error) {
	dec, err := zstd.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	result, err := dec.DecodeAll(b, nil)
	if err != nil {
		return nil, err
	}

	dec.Close()

	return result, nil
}
