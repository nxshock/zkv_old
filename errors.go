package zkv

import (
	"errors"
)

var (
	errNotFound = errors.New("not found")
	errReadOnly = errors.New("storage is read only")
)
