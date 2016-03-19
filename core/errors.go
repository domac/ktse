package core

import (
	"errors"
)

func NewError(msg string) error {
	return errors.New(msg)
}

var (
	ErrMessageType     = errors.New("message type error")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrTryMaxTimes     = errors.New("retry task max time")
	ErrFileNotExist    = errors.New("file not exist")
	ErrBadConn         = errors.New("bad net connection")
	ErrResultNotExist  = errors.New("result not exist")
	ErrExecTimeout     = errors.New("exec time out")
)
