package statstore

import (
	"strings"
	"time"
)

// Interface for things that store things.
type Storer interface {
	Insert(m map[string]interface{}, ts time.Time) (string, string, error)
	Close() error
}

// Interface for reading stored things.
type Reader interface {
	Next() (map[string]interface{}, time.Time, error)
	Close() error
}

// Get a storer for the given path.
func GetStorer(path string) (Storer, error) {
	if strings.HasPrefix(path, "http://") {
		return openCouchStorer(path)
	}
	if strings.HasSuffix(path, ".zip") {
		return openZipStorer(path)
	}
	return openFileStorer(path)
}

// Get a storer reader for the given path.
func GetStoreReader(path string) (Reader, error) {
	if strings.HasSuffix(path, ".zip") {
		return openZipReader(path)
	}
	return openFileReader(path)
}
