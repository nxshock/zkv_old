package zkv

// Config represents storage config options
type Config struct {
	BlockDataSize int64
	ReadOnly      bool
}

var defaultConfig = &Config{
	BlockDataSize: 256 * 1024,
	ReadOnly:      false}

// Config returens storage config (read only)
func (db *Db) Config() Config {
	return db.config
}
