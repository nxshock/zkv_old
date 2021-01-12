package zkv

import (
	"bytes"
	"errors"
	"io"
)

const uint64Size = 8

var errBadUint = errors.New("gob: encoded unsigned integer out of range")

// readGobData returns one gob-encoded block of data
func readGobData(r io.Reader) ([]byte, error) {
	buf := new(bytes.Buffer)

	l, b, err := getGobDataLength(r)
	if err != nil {
		return nil, err
	}

	buf.Write(b)
	_, err = io.CopyN(buf, r, int64(l))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func getGobDataLength(r io.Reader) (remainLength int, buf []byte, err error) {
	buf = make([]byte, 10)

	width := 1
	n, err := io.ReadFull(r, buf[0:width])
	if n == 0 {
		return
	}
	b := buf[0]
	if b <= 0x7f {
		return int(b), buf[:width], nil
	}
	n = -int(int8(b))
	if n > uint64Size {
		err = errBadUint
		return
	}
	width, err = io.ReadFull(r, buf[1:n+1])
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return
	}
	// Could check that the high byte is zero but it's not worth it.
	for _, b := range buf[1 : width+1] {
		remainLength = remainLength<<8 | int(b)
		if b == 0 {
			width--
		}
	}
	width++ // +1 for length byte
	buf = buf[0:width]
	return
}
