# zkv

Simple key/value storage focused on high data compression.

**Note: library is unstable, compatibility with older versions is not guaranteed.**

Keys stored in memory, while values stored on disk.

Data stored in log based structure. Records grouped into blocks to increase compression level.

Features:
* High compression ratio;

Disadvantages:
* Medium speed writes;
* Slow reads;
* Deleting or replacing data does not recover free space.
* Every request blocks whole storage.

File structure:
```
header               [3]byte
version              [1]byte
compressor id        [1]byte
block data size      [8]byte // minimal block size for compression

[]blocks
	block length     [8]byte // compressed block length
	data length      [8]byte // decompressed block length

	[]record
		action       [1]byte
		key length   [8]byte
		key          []byte  // gob-encoded
		value length [8]byte // only for records with action == actionAdd
		value        []byte  // gob-encoded, only for records with action == actionAdd
```

## Usage

**Open or create new storage:**

```go
import "github.com/nxshock/zkv"

db, err := Open("path_to_file.zkv")
defer db.Close() // don't forget to close storage
```

**Open storage with custom config:**

```go
config := &zkv.Config{
	BlockDataSize: 4 * 1024 * 1024,  // set custom block size

	Compressor:    zkv.XzCompressor, // choose from [NoneCompressor, XzCompressor, ZstdCompressor]
	                                 // or create custom compressor that match zkv.Compressor interface

	ReadOnly:      true}             // set true if storage must be read only

db, err := OpenWithConfig("path_to_file.zkv", config)
```

**List of available compressors:**

1. `zkv.XzCompressor` (default) - high compression ratio, slow speed;
2. `zkv.ZstdCompressor` - medium compression ratio, fast compression and medium speed decompression;
3. `zkv.NoneCompressor` - no compression, high speed.

**Write data:**

```go
err := db.Set(key, value) // key and value can be any type
```

**Read data:**

```go
var value ValueType
err := db.Get(key, &value)
```

**Delete data:**

```go
err := db.Delete(key) // returns nil error if key does not exists
```

**Flush data on disk (for example to prevent loosing buffered data):**

```go
err := db.Flush()
```

Often calls reduce compression ratio because written data on disk does not grouped into blocks. It you want to update data on disk on every record write, open storage with Config.BlockDataSize = 1.

**Get number of stored records:**

```go
count := db.Count()
```

**Iterate over keys and values in written order:**

```go
f := func(keyBytes, valueBytes []byte) bool {
	var key KeyType
	var value ValueType

	err := zkv.Decode(keyBytes, &key)
	err = zkv.Decode(valueBytes, &value)

	// now you can work with key and value

	return true // return true to continue iterating else return false
}

err := db.Iterate(f)
```

This provides maximum possible read speed.

**Shrink storage size by deleting overwrited records from file:**

```go
err := db.Shrink(newFilePath)
```

## Used libraries

* [xz](https://github.com/ulikunitz/xz) - Go language package supports the reading and writing of xz compressed streams;
* [zstd](https://github.com/klauspost/compress/tree/master/zstd) - provides compression to and decompression of Zstandard content.
