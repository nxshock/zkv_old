package zkv

// Config represents storage config options
type Config struct {
	BlockDataSize int64
	Compressor    Compressor
	ReadOnly      bool
}

var defaultConfig = &Config{
	BlockDataSize: 64 * 1024,
	Compressor:    ZstdCompressor,
	ReadOnly:      false}

// Config returens storage config (read only)
func (db *Db) Config() Config {
	return db.config
}
