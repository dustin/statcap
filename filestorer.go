package main

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"sync"
)

type fileStorer struct {
	lock sync.Mutex
	file *os.File
	z    *gzip.Writer
	e    *json.Encoder
}

func (ff *fileStorer) Insert(m interface{}) (string, string, error) {
	ff.lock.Lock()
	defer ff.lock.Unlock()

	return "", "", ff.e.Encode(m)
}

func (ff *fileStorer) Close() error {
	ff.lock.Lock()
	defer ff.lock.Unlock()
	defer ff.file.Close()
	return ff.z.Close()
}

func OpenFileStorer(filepath string) (*fileStorer, error) {
	f, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}
	z := gzip.NewWriter(f)
	e := json.NewEncoder(z)
	return &fileStorer{
		file: f,
		z:    z,
		e:    e,
	}, nil
}
