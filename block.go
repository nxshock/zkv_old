package zkv

import (
	"encoding/binary"
	"fmt"
	"io"
)

func writeBlock(w io.Writer, data []byte) error {
	compressedBlockData, err := compress(data)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, int64(len(compressedBlockData)))
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, int64(len(data)))
	if err != nil {
		return err
	}

	_, err = w.Write(compressedBlockData)
	if err != nil {
		return err
	}

	return nil
}

func readBlock(r io.Reader) (decompressedData []byte, err error) {
	var blockLength int64
	err = binary.Read(r, binary.LittleEndian, &blockLength)
	if err != nil {
		return nil, err
	}

	var dataLength int64
	err = binary.Read(r, binary.LittleEndian, &dataLength)
	if err != nil {
		return nil, err
	}

	if dataLength < 0 {
		return nil, fmt.Errorf("unexpected data length: %d", dataLength)
	}

	b := make([]byte, int(blockLength))

	_, err = r.Read(b)
	if err != nil {
		return nil, err
	}

	dataBytes, err := decompress(b)
	if err != nil {
		return nil, err
	}

	return dataBytes, nil
}
