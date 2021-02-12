package zkv

import (
	"errors"
)

var (
	ErrNotFound = errors.New("not found")
	errReadOnly = errors.New("storage is read only")
)
