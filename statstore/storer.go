package statstore

import (
	"strings"
)

// Interface for things that store things.
type Storer interface {
	Insert(m interface{}) (string, string, error)
	Close() error
}

// Get a storer for the given path.
func GetStorer(path string) (Storer, error) {
	if strings.HasPrefix(path, "http://") {
		return openCouchStorer(path)
	}
	return openFileStorer(path)
}
