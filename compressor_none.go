package zkv

type noneCompressor struct{}

// NoneCompressor provides no compression.
var NoneCompressor = new(noneCompressor)

func (noneC *noneCompressor) Id() int8 {
	return 1
}

func (noneC *noneCompressor) Init() error {
	return nil
}

func (noneC *noneCompressor) Compress(b []byte) ([]byte, error) {
	return b, nil
}

func (noneC *noneCompressor) Decompress(b []byte) ([]byte, error) {
	return b, nil
}
