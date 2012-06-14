package statstore

import (
	"strings"
	"time"
)

// Interface for things that store things.
type Storer interface {
	Insert(m interface{}, ts time.Time) (string, string, error)
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
