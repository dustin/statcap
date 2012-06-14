package statstore

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"sync"
	"time"
)

type fileStorer struct {
	lock sync.Mutex
	file *os.File
	z    *gzip.Writer
	e    *json.Encoder
}

func (ff *fileStorer) Insert(ob interface{}, ts time.Time) (string, string, error) {
	ff.lock.Lock()
	defer ff.lock.Unlock()

	if m, ok := ob.(map[string]interface{}); ok {
		m["ts"] = ts
	}

	return "", "", ff.e.Encode(ob)
}

func (ff *fileStorer) Close() error {
	ff.lock.Lock()
	defer ff.lock.Unlock()
	defer ff.file.Close()
	return ff.z.Close()
}

func openFileStorer(filepath string) (*fileStorer, error) {
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

type fileReader struct {
	file *os.File
	z    *gzip.Reader
	e    *json.Decoder
}

func (f *fileReader) Close() error {
	f.z.Close()
	return f.file.Close()
}

func (f *fileReader) Next() (rv interface{}, ts time.Time, err error) {
	err = f.e.Decode(&rv)
	if err != nil {
		return
	}
	if m, ok := rv.(map[string]interface{}); ok {
		switch i := m["ts"].(type) {
		case time.Time:
			ts = i
		case string:
			ts, _ = time.Parse(time.RFC3339, i)
		case nil:
			// something
		}
	}
	return
}

func openFileReader(filepath string) (*fileReader, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	rv := &fileReader{
		file: f,
	}
	rv.z, err = gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	rv.e = json.NewDecoder(rv.z)
	return rv, nil
}
