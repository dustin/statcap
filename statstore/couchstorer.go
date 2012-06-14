package statstore

import (
	"time"

	"code.google.com/p/dsallings-couch-go"
)

type couchStorer struct {
	db couch.Database
}

func (cc *couchStorer) Close() error {
	return nil
}

func (cc *couchStorer) Insert(m map[string]interface{}, ts time.Time) (string, string, error) {
	return cc.db.Insert(m)
}

func openCouchStorer(path string) (Storer, error) {
	f, err := couch.Connect(path)
	if err != nil {
		return nil, err
	}
	return &couchStorer{f}, nil
}
