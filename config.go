package zkv

type Config struct {
	BlockDataSize int64
}

var defaultConfig = &Config{
	BlockDataSize: 256 * 1024}

func (db *Db) Config() Config {
	return db.config
}
