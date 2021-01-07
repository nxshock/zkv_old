package zkv

import (
	"bytes"
	"io/ioutil"

	"github.com/ulikunitz/xz"
)

type xzCompressor struct{}

// XzCompressor provides LZMA2 compression
var XzCompressor = new(xzCompressor)

func (xzC *xzCompressor) Id() int8 {
	return 2
}

func (xzC *xzCompressor) Init() error {
	return nil
}

func (xzC *xzCompressor) Compress(b []byte) ([]byte, error) {
	buf := new(bytes.Buffer)

	encoder, err := xz.NewWriter(buf)
	if err != nil {
		return nil, err
	}

	_, err = encoder.Write(b)
	if err != nil {
		return nil, err
	}

	err = encoder.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (xzC *xzCompressor) Decompress(b []byte) ([]byte, error) {
	dec, err := xz.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(dec)
}
