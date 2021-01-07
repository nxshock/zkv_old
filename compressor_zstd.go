package zkv

import "github.com/klauspost/compress/zstd"

type zstdCompressor struct{ encoder *zstd.Encoder }

// ZstdCompressor provides Zstandard compression
var ZstdCompressor = new(zstdCompressor)

func (zstdC *zstdCompressor) Id() int8 {
	return 3
}

func (zstdC *zstdCompressor) Init() error {
	var err error
	zstdC.encoder, err = zstd.NewWriter(nil)

	return err
}

func (zstdC *zstdCompressor) Compress(b []byte) ([]byte, error) {
	return zstdC.encoder.EncodeAll(b, nil), nil
}
func (zstdC *zstdCompressor) Decompress(b []byte) ([]byte, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()

	return dec.DecodeAll(b, nil)
}
