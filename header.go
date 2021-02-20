package zkv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type header struct {
	version      int8
	compressorId int8
}

var headerLength = int64(len(headerBytes) + 1 + 1)

func writeHeader(w io.Writer, compressorId int8) error {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.LittleEndian, headerBytes)
	if err != nil {
		return err
	}

	err = binary.Write(&buf, binary.LittleEndian, version)
	if err != nil {
		return err
	}

	err = binary.Write(&buf, binary.LittleEndian, compressorId)
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(w)
	if err != nil {
		return err
	}

	return nil
}

func readHeader(r io.Reader) (*header, error) {
	rHeaderBytes := make([]byte, len(headerBytes))
	_, err := r.Read(rHeaderBytes)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(rHeaderBytes, headerBytes) {
		return nil, fmt.Errorf("wrong file header, expected %v, got %v", headerBytes, rHeaderBytes)
	}

	header := new(header)

	err = binary.Read(r, binary.LittleEndian, &header.version)
	if err != nil {
		return nil, err
	}

	err = binary.Read(r, binary.LittleEndian, &header.compressorId)
	if err != nil {
		return nil, err
	}

	return header, nil
}
