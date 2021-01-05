package zkv

import (
	"errors"
)

var (
	errReadOnly = errors.New("storage is read only")
)
