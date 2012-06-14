package statstore

import (
	"archive/zip"
	"encoding/json"
	"os"
	"sync"
	"time"
)

type zipStorer struct {
	lock sync.Mutex
	file *os.File
	z    *zip.Writer
}

func (z *zipStorer) Insert(ob interface{}) (string, string, error) {
	z.lock.Lock()
	defer z.lock.Unlock()

	fmt := "20060102T150405.json"
	ts := time.Now()
	if m, ok := ob.(map[string]interface{}); ok {
		if t, ok := m["ts"]; ok {
			ts = t.(time.Time)
		}
	}
	filename := ts.Format(fmt)

	h := zip.FileHeader{
		Name:   filename,
		Method: zip.Deflate,
	}
	h.SetModTime(ts)

	f, err := z.z.CreateHeader(&h)
	if err != nil {
		return "", "", err
	}

	err = json.NewEncoder(f).Encode(ob)
	if err != nil {
		return "", "", err
	}

	return filename, "", nil
}

func (z *zipStorer) Close() error {
	z.lock.Lock()
	defer z.lock.Unlock()
	defer z.file.Close()
	return z.z.Close()
}

func openZipStorer(filepath string) (*zipStorer, error) {
	f, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}
	z := zip.NewWriter(f)
	return &zipStorer{
		file: f,
		z:    z,
	}, nil
}
