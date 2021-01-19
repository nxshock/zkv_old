package zkv

var (
	availableCompressors map[int8]Compressor
	defaultCompressor    = XzCompressor
)

// Compressor represents compressor interface
type Compressor interface {
	Compress([]byte) ([]byte, error)
	Decompress([]byte) ([]byte, error)
	Id() int8
	Init() error
}

func init() {
	availableCompressors = make(map[int8]Compressor)

	availableCompressors[NoneCompressor.Id()] = NoneCompressor
	availableCompressors[XzCompressor.Id()] = XzCompressor
	availableCompressors[ZstdCompressor.Id()] = ZstdCompressor

	for _, compressor := range availableCompressors {
		err := compressor.Init()
		if err != nil {
			panic(err)
		}
	}
}
