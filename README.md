# logkv

Simple key/value storage focused on high data compression.

**Note: library is unstable, compatibility with older versions is not guaranteed.**

Data stored in log based structure. Records grouped into blocks to increase compression level.

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
		key          [8]byte
		value length [8]byte
		value        []byte
```
