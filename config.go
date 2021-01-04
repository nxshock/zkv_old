package zkv

type Config struct {
	BlockDataSize int64
	ReadOnly      bool
}

var defaultConfig = &Config{
	BlockDataSize: 256 * 1024,
	ReadOnly:      false}

func (db *Db) Config() Config {
	return db.config
}
