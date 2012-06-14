package statstore

import (
	"archive/zip"
	"encoding/json"
	"os"
	"sync"
	"time"
)

const timeFormat = "20060102T150405.json"

type zipStorer struct {
	lock sync.Mutex
	file *os.File
	z    *zip.Writer
}

func (z *zipStorer) Insert(ob interface{}, ts time.Time) (string, string, error) {
	z.lock.Lock()
	defer z.lock.Unlock()

	filename := ts.Format(timeFormat)

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
