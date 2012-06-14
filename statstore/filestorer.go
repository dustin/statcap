package statstore

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

func (ff *fileStorer) Insert(ob StoredItem) (string, string, error) {
	ff.lock.Lock()
	defer ff.lock.Unlock()

	// Add a timestamp if there isn't one.
	m := (*ob.rawI).(map[string]interface{})
	if _, ok := m["ts"]; !ok {
		m["ts"] = ob.Timestamp()
	}

	return "", "", ff.e.Encode(m)
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

func (f *fileReader) Next() (m StoredItem, err error) {
	err = f.e.Decode(&m)
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
