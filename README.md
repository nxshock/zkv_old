# zkv

Simple key/value storage focused on high data compression.

**Note: library is unstable, compatibility with older versions is not guaranteed.**

Keys stored in memory, while values stored on disk.

Data stored in log based structure. Records grouped into blocks to increase compression level.

Features:
* High compression ratio;
* Fast writes.

Disadvantages:
* Slow reads;
* Deleting or replacing data does not recover free space.
* Every request blocks whole storage.

File structure:
```
header               [3]byte
version              [1]byte
block data size      [8]byte // minimal block size for compression

[]blocks
	block length     [8]byte // compressed block length
	data length      [8]byte // decompressed block length

	[]record
		action       [1]byte
		key length   [8]byte
		key          [8]byte
		value length [8]byte
		value        []byte
```

## Usage

Open or create new storage:

```go
import "github.com/nxshock/zkv"

db, err := Open("path_to_file.zkv")
defer db.Close() // don't forget to close storage
```

Write data:

```go
err := db.Set(key, value) // key and value can be any type
```

Read data:

```go
var value ValueType
err := db.Get(key, &value)
```

Delete data:

```go
err := db.Delete(key) // returns nil error if key does not exists
```

Get number of stored records:

```go
count := db.Count()
```