# logkv

Simple key/value storage focused on high data compression.

**Note: library is unstable, compatibility with older versions is not guaranteed.**

File structure:
```
header               [3]byte
version              [1]byte
block data size      [8]byte

[]blocks
	block length     [8]byte
	data length      [8]byte

	[]record
		action       [1]byte
		key          [8]byte
		value length [8]byte
		value        []byte
```
