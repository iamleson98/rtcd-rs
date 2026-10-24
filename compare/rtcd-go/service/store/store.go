package store

import (
	"errors"
)

var (
	ErrNotFound = errors.New("error: not found")
	ErrEmptyKey = errors.New("error: empty key")
	ErrConflict = errors.New("error: conflict")
)

type Store interface {
	Put(key, value string) error
	Set(key, value string) error
	Get(key string) (string, error)
	Delete(key string) error
	Close() error
}

func New(dataSource string) (Store, error) {
	return newBitcaskStore(dataSource)
}
