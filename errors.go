package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("The key is empty")
	ErrIndexUpdateFailed = errors.New("failed to update index")
)
