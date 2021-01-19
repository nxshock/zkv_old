package zkv

import "io"

// Reader represents necessary methods for record reader
type reader interface {
	io.Reader
	io.ByteReader
}
