package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("The key is empty")
	ErrIndexUpdateFailed = errors.New("Failed to update index")
	ErrKeyNotFound       = errors.New("Key is not found in the database")
	ErrDataFileNotFound  = errors.New("Data file is not found")
)
