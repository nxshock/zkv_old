package zkv

import "io"

// Reader represents necessary methods for record reader
type Reader interface {
	io.Reader
	io.ByteReader
}
