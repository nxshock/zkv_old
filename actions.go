package zkv

type action int8

const (
	actionNone action = iota
	actionAdd
	actionDelete
)
