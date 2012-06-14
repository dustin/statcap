package statstore

import (
	"archive/zip"
	"encoding/json"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

const timeFormat = "20060102T150405.000.json"

type zipStorer struct {
	lock sync.Mutex
	file *os.File
	z    *zip.Writer
}

func (z *zipStorer) Insert(ob StoredItem) (string, string, error) {
	z.lock.Lock()
	defer z.lock.Unlock()

	ts := ob.Timestamp()
	filename := ts.Format(timeFormat)

	h := zip.FileHeader{
		Name:   filename,
		Method: zip.Deflate,
		Extra:  []byte(ts.Format(time.RFC3339Nano)),
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

type fileList []*zip.File

func (f fileList) Len() int {
	return len(f)
}

func (f fileList) Less(i, j int) bool {
	return f[i].Comment < f[j].Comment
}

func (f fileList) Swap(i, j int) {
	f[j], f[i] = f[i], f[j]
}

type ZipReader struct {
	z *zip.ReadCloser

	files   fileList
	current int
}

func (z *ZipReader) Close() error {
	return z.z.Close()
}

func (z *ZipReader) Next() (StoredItem, error) {
	rv := StoredItem{}

	if z.current >= len(z.files) {
		return rv, io.EOF
	}

	defer func() {
		z.current++
	}()

	r, err := z.files[z.current].Open()
	if err != nil {
		return rv, err
	}
	defer r.Close()

	err = json.NewDecoder(r).Decode(&rv)
	if err != nil {
		return rv, err
	}

	ts, err := time.Parse(time.RFC3339Nano,
		string(z.files[z.current].Extra))

	rv.ts = &ts

	return rv, err
}

func openZipReader(filepath string) (*ZipReader, error) {
	f, err := zip.OpenReader(filepath)
	if err != nil {
		return nil, err
	}

	files := make(fileList, len(f.File))
	copy(files, f.File)
	sort.Sort(files)

	return &ZipReader{
		z:     f,
		files: files,
	}, nil
}
