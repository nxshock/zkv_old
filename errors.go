package zkv

import (
	"errors"
)

var (
	errReadOnly = errors.New("database is read only")
)
